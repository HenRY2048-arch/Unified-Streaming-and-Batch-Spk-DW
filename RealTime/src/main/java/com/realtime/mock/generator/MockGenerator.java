package com.realtime.mock.generator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 行为流生成（P1.3 会话旅途 + P1.4 归因真值）。
 *
 * 一次 {@link #generateNext()} 推进某个用户"一步"：
 *   IDLE      → 起会话（app_open）
 *   BROWSING  → 分发（exposure_batch）→ 点击判定 → enter_item（可能不点）
 *   CONSUMING → leave_item（采样停留）→ 互动/关注（真正改图）/ 进主页 / 点标签 / 私信 → 回 BROWSING
 *
 * 每次转化（互动 / 关注）同时产出 **归因真值**（{@link GroundTruth}，走独立 topic，不进事件流）。
 */
public class MockGenerator {

    private enum Stage { IDLE, BROWSING, CONSUMING }

    /** 单次调用最多补齐多少个新用户（防止长时间空闲后一次性灌爆）。 */
    private static final int MAX_REGISTRATIONS_PER_CALL = 500;

    private static final class SessionState {
        Stage stage = Stage.IDLE;
        int steps;
        String batchId;
        String channel;
        int position;
        Item item;
    }

    private final DataManager dataManager;
    private final DistributionService distribution;
    private final Random random = new Random();
    private final AtomicLong globalCounter = new AtomicLong(0);
    private final Map<Long, SessionState> sessions = new HashMap<>();
    private final List<Long> activeUsers = new ArrayList<>();
    private final Map<Long, Integer> activePos = new HashMap<>();
    private final InteractionState interactionState = new InteractionState();
    private final DateTimeFormatter isoFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    /** P1.6：上次注入新用户的事件时间；新用户计数（仅运行期统计）。 */
    private long lastRegisteredSimMs;
    private long registeredUsers = 0L;
    /** 本次运行的事件时间起点（用于收尾统计"推进了多少事件时间"）。 */
    private final long clockStartMs;
    /** 待"首次打开"的新用户（按 activationSimMs 排序）：注册后系统会给一次开机，保证 cohort 起点存在。 */
    private final PriorityQueue<User> pendingFirstOpen = new PriorityQueue<>(Comparator.comparingLong(User::getActivationSimMs));

    public MockGenerator(DataManager dataManager) {
        this.dataManager = dataManager;
        this.distribution = new DistributionService(dataManager, random);
        this.lastRegisteredSimMs = SimClock.peek();
        this.clockStartMs = SimClock.peek();
    }

    public GeneratedBatch generateNext() {
        long count = globalCounter.incrementAndGet();

        if (count % 1000 == 0) return new GeneratedBatch(generateA1SpamBurst(), Collections.emptyList());
        if (count % 1000 == 500) return new GeneratedBatch(generateA2CrazyBurst(), Collections.emptyList());

        List<EventLog> events = new ArrayList<>(2);
        List<GroundTruth> truths = new ArrayList<>(1);

        // P1.6：新用户零散注入（与"作者产出新内容"对称）+ 到点后的"首次打开"
        maybeRegisterNewUser(events);
        emitDueFirstOpens(events);

        // 供给侧：作者产出新内容
        if (random.nextDouble() < AppConfig.ITEM_PUBLISH_RATE) {
            EventLog published = generateItemPublished(dataManager.getRandomCreator());
            if (published != null) events.add(published);
        }

        User user = pickUser();
        SessionState st = sessions.computeIfAbsent(user.getUserId(), k -> new SessionState());

        // 收口"过期会话"：会话内只按自己的停留/思考推进，若它已落后全局时钟超过一个会话超时，
        // 说明这条时间线已被甩开（长尾会到小时级），直接结束会话、下次重新锚定，把偏差压在超时以内。
        if (st.stage != Stage.IDLE
                && SimClock.now() - user.getEventTimeMs() > AppConfig.sessionTimeoutMs()) {
            endSession(user, st);
            // 注意：不能返回空批次——本次调用里已经生成的 user_registered / item_published 必须保留，
            // 否则"CSV 已落盘、topic 没收到"（实测丢了近一半注册事件）。
            return new GeneratedBatch(events, Collections.emptyList());
        }

        switch (st.stage) {
            case IDLE:
                startSession(user, st);
                activate(user.getUserId());
                user.bumpActivity();
                events.add(baseEvent(user, "app_open"));
                break;
            case BROWSING:
                events.addAll(browse(user, st));
                break;
            case CONSUMING:
                consume(user, st, events, truths);
                break;
        }
        return new GeneratedBatch(events, truths);
    }

    /** C5：分发成本统计（供收尾时打印）。 */
    public String costStats() {
        double rankMs = distribution.getRankNanos() / 1_000_000.0;
        return "batches=" + distribution.getBatchesBuilt()
                + " scored=" + distribution.getCandidatesScored()
                + " rankMs=" + String.format("%.1f", rankMs)
                + " costTier=" + AppConfig.COST_TIER
                + " strategy=" + AppConfig.DISTRIBUTION_STRATEGY
                + " registeredUsers=" + registeredUsers
                + " eventTimeElapsedH=" + String.format("%.2f",
                        (SimClock.peek() - clockStartMs) / 3_600_000.0);
    }

    // ---------------- 会话旅途 ----------------

    private void startSession(User user, SessionState st) {
        // 起会话＝把用户时间线**锚定**到全局事件时钟（会话之间流逝了多少时间，由全局时钟决定）
        long now = SimClock.observe(Math.max(user.getEventTimeMs(), SimClock.now()));
        user.setCurrentSessionId("sess_" + now + "_" + user.getUserId());
        user.setLastActiveTime(now);
        user.setEventTimeMs(now);
        st.stage = Stage.BROWSING;
        st.steps = 0;
        st.item = null;
    }

    /**
     * 会话内的时间推进（1:1，不乘任何倍率）：用户时钟按模型时长前进，全局时钟按同一份模型时长前进，
     * 两者同源；{@code leave_ts − enter_ts} 因此恒等于 {@code duration_sec}。
     */
    private void advanceTime(User user, long modelMs) {
        if (modelMs <= 0) return;
        user.advanceEventTime(modelMs);
        SimClock.advanceModel(modelMs);
    }

    /** 优先推进"活跃会话"，否则开新会话。 */
    private User pickUser() {
        boolean atCap = activeUsers.size() >= AppConfig.SESSION_MAX_OPEN;
        // 达到并发上限时只推进已有会话——否则并发会话越多，单个会话越久才轮到下一步，
        // 全局时钟（随所有会话的模型时长前进）会把它们远远甩开。
        if (!activeUsers.isEmpty() && (atCap || random.nextDouble() < AppConfig.SESSION_RESUME_WEIGHT)) {
            User u = dataManager.getUser(activeUsers.get(random.nextInt(activeUsers.size())));
            if (u != null) return u;
        }
        return dataManager.getRandomUser();
    }

    private void activate(long userId) {
        if (!activePos.containsKey(userId)) {
            activePos.put(userId, activeUsers.size());
            activeUsers.add(userId);
        }
    }

    private void deactivate(long userId) {
        Integer idx = activePos.remove(userId);
        if (idx == null) return;
        int last = activeUsers.size() - 1;
        Long moved = activeUsers.get(last);
        activeUsers.set(idx, moved);
        activePos.put(moved, idx);
        activeUsers.remove(last);
    }

    private void endSession(User user, SessionState st) {
        st.stage = Stage.IDLE;
        st.item = null;
        deactivate(user.getUserId());
    }

    private List<EventLog> browse(User user, SessionState st) {
        user.bumpActivity();

        // 会话内思考时间：推进"该用户"的事件时钟（事件时间轴上"刷下一条"不是瞬时的）
        advanceTime(user, sampleThinkMs());
        if (++st.steps > AppConfig.SESSION_MAX_STEPS) {
            endSession(user, st);
            return Collections.emptyList();
        }

        DistributionService.Batch batch = distribution.buildBatch(user);
        if (batch.candidates.isEmpty()) {
            endSession(user, st);
            return Collections.emptyList();
        }

        String batchId = "bch_" + UUID.randomUUID().toString().replace("-", "");
        List<EventLog> out = new ArrayList<>(2);

        if (AppConfig.EMIT_EXPOSURE) {
            EventLog exp = baseEvent(user, "exposure_batch");
            exp.setBatchId(batchId);
            exp.setChannel(batch.channel);
            List<ExposureItem> items = new ArrayList<>(batch.candidates.size());
            for (DistributionService.Candidate c : batch.candidates) {
                ExposureItem ei = new ExposureItem();
                ei.setItemId(c.itemId);
                ei.setPosition(c.position);
                if (AppConfig.BATCH_INCLUDE_RANK_SCORE) ei.setRankScore(round3(c.score));
                items.add(ei);
            }
            exp.setItems(items);
            out.add(exp);
        }

        DistributionService.Candidate clicked = distribution.clickOrNull(batch, user);
        if (clicked == null) {
            distribution.noteOutcome(batch.channel, 0.0);
            return out;
        }

        Item item = dataManager.getItem(clicked.itemId);
        if (item == null) return out;

        EventLog enter = baseEvent(user, "enter_item");
        enter.setItemId(item.getItemId());
        enter.setChannel(batch.channel);
        enter.setBatchId(batchId);
        enter.setPosition(clicked.position);
        out.add(enter);

        st.item = item;
        st.channel = batch.channel;
        st.batchId = batchId;
        st.position = clicked.position;
        st.stage = Stage.CONSUMING;
        return out;
    }

    private void consume(User user, SessionState st, List<EventLog> out, List<GroundTruth> truths) {
        user.bumpActivity();
        Item item = st.item;

        double dwell = sampleDwell(item);
        // 关键：停留时长 1:1 计入"该用户"的事件时钟 → leave_ts − enter_ts 恒等于 duration_sec
        advanceTime(user, (long) (dwell * 1000));
        EventLog leave = baseEvent(user, "leave_item");
        leave.setItemId(item.getItemId());
        leave.setDurationSec(round2(dwell));
        out.add(leave);

        boolean engaged = false;
        long uid = user.getUserId();
        long iid = item.getItemId();

        // —— 状态型：`like` / `dislike` 对 (user,item) **一次性**且**互斥** ——
        InteractionState.Stance stance = interactionState.stanceOf(uid, iid);
        if (stance == InteractionState.Stance.NONE) {
            double pLike = prob(AppConfig.ENGAGE_LIKE, item, user, dwell);
            double pDislike = prob(AppConfig.ENGAGE_DISLIKE, item, user, dwell);
            double roll = random.nextDouble();
            if (roll < pLike) {
                out.add(convert(user, "like", item, st, truths));
                interactionState.markLike(uid, iid);
                stance = InteractionState.Stance.LIKE;
                engaged = true;
            } else if (roll < pLike + pDislike) {
                out.add(convert(user, "dislike", item, st, truths));
                interactionState.markDislike(uid, iid);
                stance = InteractionState.Stance.DISLIKE;
            }
        }

        // —— 状态型：`fav` 一次性；且"不感兴趣"时不收藏 ——
        if (stance != InteractionState.Stance.DISLIKE
                && !interactionState.isFaved(uid, iid)
                && random.nextDouble() < prob(AppConfig.ENGAGE_FAV, item, user, dwell)) {
            out.add(convert(user, "fav", item, st, truths));
            interactionState.markFaved(uid, iid);
            engaged = true;
        }

        // —— 动作型：可重复（分享可多次、评论可多次）——
        if (random.nextDouble() < prob(AppConfig.ENGAGE_SHARE, item, user, dwell)) {
            out.add(convert(user, "share", item, st, truths));
            engaged = true;
        }
        if (random.nextDouble() < prob(AppConfig.ENGAGE_COMMENT, item, user, dwell)) {
            EventLog c = convert(user, "comment", item, st, truths);
            c.setCommentText(AppConfig.COMMENTS[random.nextInt(AppConfig.COMMENTS.length)]);
            out.add(c);
            engaged = true;
        }

        // 关注转化：从"正在看的内容"关注其作者（真正改图）
        long author = item.getAuthorId();
        if (author != user.getUserId()
                && !dataManager.isFollowing(user.getUserId(), author)
                && random.nextDouble() < followProb(item, user, dwell)) {
            EventLog f = baseEvent(user, "follow");
            f.setTargetUserId(author);
            f.setChannel(st.channel);
            f.setSourceItemId(item.getItemId());
            f.setSourceUserId(author);
            out.add(f);
            truths.add(truthFor(f, item, st, user, "follow"));
            dataManager.addFollow(user.getUserId(), author);
            engaged = true;
        }

        // 取关（低频）
        Set<Long> following = dataManager.getFollowing(user.getUserId());
        if (!following.isEmpty() && random.nextDouble() < AppConfig.UNFOLLOW_PROB) {
            List<Long> list = new ArrayList<>(following);
            long target = list.get(random.nextInt(list.size()));
            EventLog uf = baseEvent(user, "unfollow");
            uf.setTargetUserId(target);
            out.add(uf);
            dataManager.removeFollow(user.getUserId(), target);
        }

        // 辅助行为：都带上"从哪条内容来"
        if (random.nextDouble() < 0.03) {
            EventLog p = baseEvent(user, "enter_profile");
            p.setTargetUserId(item.getAuthorId());
            p.setSourceItemId(item.getItemId());
            out.add(p);
        }
        if (random.nextDouble() < 0.02 && item.getTags() != null && !item.getTags().isEmpty()) {
            EventLog t = baseEvent(user, "click_tag");
            t.setTag(item.getTags().get(random.nextInt(item.getTags().size())));
            t.setItemId(item.getItemId());
            out.add(t);
        }
        if (random.nextDouble() < 0.01) {
            EventLog dm = baseEvent(user, "dm");
            dm.setTargetUserId(pickDmTarget(user, item));
            dm.setCommentText(AppConfig.DM_CONTENTS[random.nextInt(AppConfig.DM_CONTENTS.length)]);
            out.add(dm);
        }

        distribution.noteOutcome(st.channel, engaged ? 1.0 : 0.0);

        st.item = null;
        if (st.steps >= AppConfig.SESSION_MAX_STEPS) endSession(user, st);
        else st.stage = Stage.BROWSING;
    }

    /** 产出一次互动事件，并登记其归因真值。 */
    private EventLog convert(User user, String type, Item item, SessionState st, List<GroundTruth> truths) {
        EventLog e = baseEvent(user, type);
        e.setItemId(item.getItemId());
        e.setBatchId(st.batchId);
        truths.add(truthFor(e, item, st, user, type));
        return e;
    }

    private GroundTruth truthFor(EventLog conversion, Item item, SessionState st, User user, String type) {
        GroundTruth t = new GroundTruth();
        t.setTruthId("tru_" + UUID.randomUUID().toString().replace("-", ""));
        t.setEventId(conversion.getEventId());
        t.setUserId(user.getUserId());
        t.setConversionType(type);
        t.setItemId(item.getItemId());
        if ("follow".equals(type)) t.setTargetUserId(item.getAuthorId());
        t.setChannel(st.channel);
        t.setBatchId(st.batchId);
        t.setPosition(st.position);
        t.setSessionId(user.getCurrentSessionId());
        t.setTs(conversion.getTs());
        return t;
    }

    // ---------------- 供给侧 ----------------

    private EventLog generateItemPublished(User user) {
        if (!user.isCreator()) return null;
        startSessionIfNone(user);

        String category = user.getPrimaryCategory();
        if (category == null || category.isEmpty()) {
            category = AppConfig.CATEGORY_TAGS[random.nextInt(AppConfig.CATEGORY_TAGS.length)][0];
        }

        List<String> tags = new ArrayList<>();
        String[] catTags = findCatTags(category);
        if (catTags != null && catTags.length > 1) {
            List<String> pool = new ArrayList<>(Arrays.asList(catTags).subList(1, catTags.length));
            Collections.shuffle(pool, random);
            tags.addAll(pool.subList(0, 1 + random.nextInt(Math.min(3, pool.size()))));
        }

        long itemId = dataManager.nextItemId();
        int contentLen = 50 + random.nextInt(4951);
        String publishedTs = formatTs(user.getEventTimeMs() > 0 ? user.getEventTimeMs() : SimClock.peek());

        dataManager.addItem(new Item(itemId, user.getUserId(), category, tags, contentLen, publishedTs));

        EventLog event = baseEvent(user, "item_published");
        event.setItemId(itemId);
        event.setAuthorId(user.getUserId());
        event.setCategory(category);
        event.setTags(String.join(",", tags));
        event.setContentLen(contentLen);
        event.setPublishedTs(publishedTs);
        return event;
    }

    private void startSessionIfNone(User user) {
        long now = SimClock.observe(Math.max(user.getEventTimeMs(), SimClock.now()));
        if (user.getCurrentSessionId() == null || (now - user.getLastActiveTime() > AppConfig.sessionTimeoutMs())) {
            now = SimClock.observe(now);
            user.setCurrentSessionId("sess_" + now + "_" + user.getUserId());
        }
        user.setEventTimeMs(now);
        user.setLastActiveTime(now);
    }

    // ---------------- 异常注入（沿用） ----------------

    private List<EventLog> generateA1SpamBurst() {
        List<EventLog> events = new ArrayList<>();
        User user = dataManager.getRandomUser();
        startSessionIfNone(user);
        Item item = dataManager.getRandomItem();
        long baseTs = user.getEventTimeMs();
        for (int i = 0; i < 5; i++) {
            EventLog event = baseEvent(user, "comment");
            event.setItemId(item.getItemId());
            event.setCommentText(AppConfig.COMMENTS[random.nextInt(AppConfig.COMMENTS.length)]);
            event.setTs(formatTs(baseTs + i * 1000L));
            events.add(event);
        }
        return events;
    }

    private List<EventLog> generateA2CrazyBurst() {
        List<EventLog> events = new ArrayList<>();
        User user = dataManager.getRandomUser();
        startSessionIfNone(user);
        long baseTs = user.getEventTimeMs();
        for (int i = 0; i < 15; i++) {
            EventLog event = baseEvent(user, "like");
            long iid = dataManager.getRandomItem().getItemId();
            event.setItemId(iid);
            event.setTs(formatTs(baseTs + random.nextInt(5000)));
            events.add(event);
            interactionState.markLike(user.getUserId(), iid);   // 异常注入同样落到状态里，保持一致
        }
        return events;
    }

    // ---------------- 模型 ----------------

    // ---- P1.6：模拟时钟 + 新用户注入 ----

    /** 会话内"思考时间"（事件毫秒）：模拟时钟按它推进，让会话时间轴具备真实感。 */
    private long sampleThinkMs() {
        double sec = AppConfig.CLOCK_THINK_MIN_SECONDS
                + random.nextDouble() * (AppConfig.CLOCK_THINK_MAX_SECONDS - AppConfig.CLOCK_THINK_MIN_SECONDS);
        return (long) (sec * 1000);
    }

    /** 注册 → 首次打开的间隔（事件毫秒），决定 U-03 的次留基线。 */
    private long sampleFirstOpenDelayMs() {
        int lo = AppConfig.USER_FIRST_OPEN_DELAY_MIN_MINUTES;
        int hi = Math.max(lo, AppConfig.USER_FIRST_OPEN_DELAY_MAX_MINUTES);
        return (lo + random.nextInt(hi - lo + 1)) * 60_000L;
    }

    /**
     * 按"每事件天 N 人"的速率零散注入新用户（与 {@code item_published} 新增内容完全对称）。
     *
     * <p>事件 {@code user_registered} **不带 {@code session_id}**（账户事件发生在会话之前；
     * 新用户此刻 {@code currentSessionId == null}，{@code @JsonInclude(NON_NULL)} 会自动省略该字段）。
     * 用户属性用**与种子同一套分布**采样，保证新老 cohort 可比。
     */
    public void maybeRegisterNewUser(List<EventLog> out) {
        if (AppConfig.USER_REGISTERED_PER_DAY <= 0) return;
        long intervalMs = (long) (86_400_000.0 / AppConfig.USER_REGISTERED_PER_DAY);
        if (intervalMs <= 0) return;
        long simNow = SimClock.peek();
        if (simNow - lastRegisteredSimMs < intervalMs) return;

        // 关键：全局时钟有很大一部分是"引擎空闲"推进的（生成器在 sleep 时事件时间仍在走），
        // 若每次调用只补 1 人，注册速率会被循环节奏卡住（实测加速档少了 ~5 倍）。
        // 因此按事件时间**补齐**：把欠下的名额一次发完，registeredTs 按 intervalMs 均匀铺开。
        int guard = 0;
        while (simNow - lastRegisteredSimMs >= intervalMs && guard++ < MAX_REGISTRATIONS_PER_CALL) {
            lastRegisteredSimMs += intervalMs;
            long regTs = lastRegisteredSimMs;

            long userId = dataManager.nextInjectUserId();
            User user = MasterDataSeeder.sampleUser(userId, random, MasterDataSeeder.tagToCategory());
            user.setSource("registered");
            user.setRegisteredTs(regTs);
            user.setEventTimeMs(regTs);          // 该用户的事件时钟从注册时刻起算
            user.setActivationSimMs(regTs + sampleFirstOpenDelayMs());
            dataManager.registerUser(user);
            pendingFirstOpen.add(user);

            EventLog e = baseEvent(user, "user_registered");
            e.setTs(formatTs(regTs));            // 注册事件时间＝其注册时刻（基类取用户时钟，这里保持一致）
            e.setInterestTags(user.interestTags());
            e.setLevel(user.getLevel());
            e.setIsCreator(user.isCreator());
            e.setPrimaryCategory(user.getPrimaryCategory());
            e.setPostRateLevel(user.getPostRateLevel());
            out.add(e);
            registeredUsers++;
        }
    }

    /**
     * 到点的"首次打开"：注册 → 首次打开的间隔到点后，系统给该用户一次开机（{@code app_open}）。
     *
     * <p>为什么必须显式发：否则新用户只是"进了池子"，能不能被抽到全靠随机——实测只有约 19% 的新用户
     * 产生过行为，U-03 的 cohort 会大面积空掉。给一次开机后，该用户进入活跃会话池，
     * 后续会按"已有会话推进"的权重被持续推进，cohort 才有意义。
     */
    private void emitDueFirstOpens(List<EventLog> out) {
        long simNow = SimClock.peek();
        while (!pendingFirstOpen.isEmpty() && pendingFirstOpen.peek().getActivationSimMs() <= simNow) {
            User user = pendingFirstOpen.poll();
            SessionState st = sessions.computeIfAbsent(user.getUserId(), k -> new SessionState());
            // 首次打开发生在"激活时刻"
            user.setEventTimeMs(Math.max(user.getEventTimeMs(), user.getActivationSimMs()));
            startSession(user, st);
            activate(user.getUserId());
            user.bumpActivity();
            out.add(baseEvent(user, "app_open"));
        }
    }

    public long getRegisteredUsers() {
        return registeredUsers;
    }

    private double sampleDwell(Item item) {
        double r = random.nextDouble();
        if (r < 0.15) return 0.2 + random.nextDouble();
        if (r < 0.20) return 0.1 + random.nextDouble() * 0.6;
        if (r < 0.205) return 1800 + random.nextInt(1801);
        double speed = AppConfig.DWELL_CHARS_PER_SEC_MIN
                + random.nextDouble() * (AppConfig.DWELL_CHARS_PER_SEC_MAX - AppConfig.DWELL_CHARS_PER_SEC_MIN);
        double dwell = AppConfig.DWELL_BASE_SECONDS + item.getContentLen() / speed;
        return dwell * (0.6 + 0.8 * random.nextDouble());
    }

    /** 概率（不掷骰）：受"兴趣匹配 + 停留时长"调制。 */
    private double prob(double base, Item item, User user, double dwell) {
        double p = base;
        if (distribution.interestMatch(item, user)) p *= 1.5;
        p *= 1.0 + Math.min(2.0, dwell / 60.0);
        return Math.min(0.95, p);
    }

    /** 私信目标：优先"关注的人"，其次"当前内容的作者"，最后才随机。 */
    private long pickDmTarget(User user, Item item) {
        Set<Long> following = dataManager.getFollowing(user.getUserId());
        if (!following.isEmpty()) {
            List<Long> list = new ArrayList<>(following);
            return list.get(random.nextInt(list.size()));
        }
        long author = item.getAuthorId();
        if (author != user.getUserId()) return author;
        return pickTargetUser(user.getUserId());
    }

    private double followProb(Item item, User user, double dwell) {
        double p = AppConfig.FOLLOW_PROB;
        if (distribution.interestMatch(item, user)) p *= 2.0;
        p *= 1.0 + Math.min(2.0, dwell / 60.0);
        return p;
    }

    // ---------------- 基础 ----------------

    private EventLog baseEvent(User user, String eventType) {
        EventLog event = new EventLog();
        event.setSchemaVersion(AppConfig.SCHEMA_VERSION);
        event.setEventId("evt_" + UUID.randomUUID().toString().replace("-", ""));
        event.setUserId(user.getUserId());
        event.setEventType(eventType);
        event.setSessionId(user.getCurrentSessionId());
        event.setPlatform(getRandomPlatform());

        // 事件时间戳取自"该用户自己的时钟"（保证与 duration_sec 自洽）
        long ts = user.getEventTimeMs() > 0 ? user.getEventTimeMs() : SimClock.peek();
        if (random.nextDouble() < 0.1) ts -= (30 + random.nextInt(61)) * 1000;   // B1 乱序
        event.setTs(formatTs(ts));
        return event;
    }

    private String getRandomPlatform() {
        String[] platforms = {"ios", "android", "web"};
        return platforms[random.nextInt(platforms.length)];
    }

    private long pickTargetUser(long selfId) {
        // P1.6：不再按固定 id 区间取值（用户池会增长），改从"已上线用户"里取
        for (int i = 0; i < 20; i++) {
            long id = dataManager.getRandomUser().getUserId();
            if (id != selfId) return id;
        }
        return selfId;
    }

    private String[] findCatTags(String category) {
        for (String[] ct : AppConfig.CATEGORY_TAGS) {
            if (ct[0].equals(category)) return ct;
        }
        return null;
    }

    private String formatTs(long ts) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("GMT+8")).format(isoFormatter);
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private static double round3(double v) { return Math.round(v * 1000.0) / 1000.0; }
}
