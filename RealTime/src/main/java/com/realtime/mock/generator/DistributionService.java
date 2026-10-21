package com.realtime.mock.generator;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 分发控制器（P1.3 真实现 + P1.4 策略/成本可配）。
 *
 *  - C2：输入＝用户 + 图 + 内容池 → 输出＝候选集与排序；
 *  - C4：**策略可切换** `hybrid | random | interest | graph`（走配置）；
 *  - C5：**成本可配且可观测** `cheap | standard | heavy`；新鲜度不再重复解析字符串；避免整表复制；
 *  - C3：动态比例（成熟度 × 渠道反馈 EMA）；A4：关注流取自图邻居作者。
 */
public class DistributionService {

    public static class Candidate {
        public long itemId;
        public int position;
        public double score;
    }

    public static class Batch {
        public String channel;
        public List<Candidate> candidates;
    }

    private static final double EMA_ALPHA = 0.05;
    private static final double EMA_INIT = 0.5;
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static final int FOLLOW_POOL_AUTHORS = 8;   // 关注流：抽样若干邻居作者，避免全量拼接

    private final DataManager data;
    private final Random rnd;
    private final Map<String, Double> channelEngagement = new HashMap<>();
    private final Map<Long, Long> publishedEpochCache = new HashMap<>();   // C5：缓存发布时间（免重复解析）

    // C5：可观测的分发成本
    private long batchesBuilt;
    private long candidatesScored;
    private long rankNanos;

    public DistributionService(DataManager data, Random rnd) {
        this.data = data;
        this.rnd = rnd;
    }

    // ---------------- 对外接口 ----------------

    public Batch buildBatch(User user) {
        batchesBuilt++;
        String channel = effectiveChannel(sampleChannel(user));
        List<Item> pool = poolFor(channel, user);
        if (pool.isEmpty()) {
            channel = "explore";
            pool = data.getItems();
        }

        Batch b = new Batch();
        b.channel = channel;
        b.candidates = new ArrayList<>();
        if (pool.isEmpty()) return b;

        int k = Math.min(AppConfig.BATCH_SIZE, pool.size());
        int sampleSize = "heavy".equals(AppConfig.COST_TIER) ? Math.min(pool.size(), k * 3) : k;

        LinkedHashSet<Long> picked = new LinkedHashSet<>();
        int guard = 0;
        while (picked.size() < sampleSize && guard++ < sampleSize * 20) {
            picked.add(pool.get(rnd.nextInt(pool.size())).getItemId());
        }

        for (Long id : picked) {
            Item it = data.getItem(id);
            if (it == null) continue;
            Candidate c = new Candidate();
            c.itemId = id;
            c.score = scoreFor(it, user, channel);
            b.candidates.add(c);
        }

        if ("cheap".equals(AppConfig.COST_TIER)) {
            Collections.shuffle(b.candidates, rnd);          // 便宜档：不排序
        } else {
            b.candidates.sort((x, y) -> Double.compare(y.score, x.score));
        }
        if (b.candidates.size() > k) {
            b.candidates = new ArrayList<>(b.candidates.subList(0, k));
        }
        for (int i = 0; i < b.candidates.size(); i++) b.candidates.get(i).position = i;
        return b;
    }

    /** D1：显式排序函数（可解释、可实验）。 */
    public double rankScore(Item item, User user, String channel) {
        double freshness = freshnessOf(item);
        double interest = interestMatch(item, user) ? 1.0 : 0.0;
        double influence = Math.min(1.0, data.followerCount(item.getAuthorId()) / 50.0);
        return 0.40 * freshness + 0.35 * interest + 0.15 * influence + 0.10 * rnd.nextDouble();
    }

    /** 位置偏置 + 兴趣加成的点击；可能不点（返回 null）。 */
    public Candidate clickOrNull(Batch batch, User user) {
        for (Candidate c : batch.candidates) {
            double p = AppConfig.CLICK_BASE * Math.pow(AppConfig.CLICK_POSITION_DECAY, c.position);
            if (interestMatch(data.getItem(c.itemId), user)) p *= AppConfig.CLICK_INTEREST_BOOST;
            if (rnd.nextDouble() < Math.min(0.95, p)) return c;
        }
        return null;
    }

    /** 渠道反馈（EMA），驱动 C3。 */
    public void noteOutcome(String channel, double engaged) {
        double cur = channelEngagement.getOrDefault(channel, EMA_INIT);
        channelEngagement.put(channel, cur + EMA_ALPHA * (engaged - cur));
    }

    /** C3：动态比例（基线 45/35/20 × 成熟度 × 渠道反馈），返回 {recommend, follow_feed, explore}。 */
    public double[] weightsFor(User user) {
        double baseFollow = AppConfig.CHANNEL_WEIGHT_FOLLOW;         // 0.35
        double baseRecommend = AppConfig.CHANNEL_WEIGHT_RECOMMEND;   // 0.45
        double baseExplore = Math.max(0.0, 1.0 - baseFollow - baseRecommend);  // 0.20

        double maturity = Math.min(1.0, user.getActivityScore() / 12.0);
        double follow = baseFollow + 0.15 * maturity;
        double explore = Math.max(0.02, baseExplore - 0.05 * maturity);
        double recommend = Math.max(0.05, 1.0 - follow - explore);

        double nf = channelEngagement.getOrDefault("follow_feed", EMA_INIT);
        double nr = channelEngagement.getOrDefault("recommend", EMA_INIT);
        double ne = channelEngagement.getOrDefault("explore", EMA_INIT);
        double avg = (nf + nr + ne) / 3.0;
        if (avg <= 0) avg = EMA_INIT;

        follow *= nf / avg;
        recommend *= nr / avg;
        explore *= ne / avg;

        double sum = follow + recommend + explore;
        if (sum <= 0) return new double[]{0.45, 0.35, 0.20};
        return new double[]{recommend / sum, follow / sum, explore / sum};
    }

    public boolean interestMatch(Item item, User user) {
        if (item == null || item.getTags() == null || user.interestTags().isEmpty()) return false;
        for (String t : item.getTags()) if (user.interestTags().contains(t)) return true;
        return false;
    }

    // ---- C5：成本可观测 ----
    public long getBatchesBuilt() { return batchesBuilt; }
    public long getCandidatesScored() { return candidatesScored; }
    public long getRankNanos() { return rankNanos; }

    // ---------------- 内部 ----------------

    /** C4：策略覆盖渠道。hybrid 走动态比例，其余策略锁定单一渠道。 */
    private String effectiveChannel(String sampled) {
        switch (AppConfig.DISTRIBUTION_STRATEGY) {
            case "random":
                return "explore";
            case "interest":
                return "recommend";
            case "graph":
                return "follow_feed";
            default:
                return sampled;
        }
    }

    private String sampleChannel(User user) {
        double[] w = weightsFor(user);
        double r = rnd.nextDouble();
        if (r < w[0]) return "recommend";
        if (r < w[0] + w[1]) return "follow_feed";
        return "explore";
    }

    private List<Item> poolFor(String channel, User user) {
        switch (channel) {
            case "follow_feed": {
                Set<Long> following = data.getFollowing(user.getUserId());
                if (following.isEmpty()) return Collections.emptyList();
                List<Long> authors = new ArrayList<>(following);
                Collections.shuffle(authors, rnd);
                List<Item> pool = new ArrayList<>();
                int n = Math.min(FOLLOW_POOL_AUTHORS, authors.size());
                for (int i = 0; i < n; i++) pool.addAll(data.getItemsByAuthor(authors.get(i)));
                return pool;
            }
            case "recommend": {
                if (user.interestTags().isEmpty()) return Collections.emptyList();
                String tag = user.interestTags().get(rnd.nextInt(user.interestTags().size()));
                return data.getItemsByTag(tag);
            }
            default:
                return data.getItems();
        }
    }

    private double scoreFor(Item item, User user, String channel) {
        candidatesScored++;
        if ("cheap".equals(AppConfig.COST_TIER)) return rnd.nextDouble();
        long t0 = System.nanoTime();
        double s = rankScore(item, user, channel);
        rankNanos += System.nanoTime() - t0;
        return s;
    }

    /** C5：缓存发布时间，避免每次都解析时间字符串。 */
    private double freshnessOf(Item item) {
        Long pub = publishedEpochCache.get(item.getItemId());
        if (pub == null) {
            try {
                pub = OffsetDateTime.parse(item.getPublishedTs(), TS_FMT).toInstant().toEpochMilli();
            } catch (Exception e) {
                pub = SimClock.now();
            }
            publishedEpochCache.put(item.getItemId(), pub);
        }
        // 新鲜度必须用模拟时钟：否则 published_ts 走在"未来"、age 归零 → 所有内容都"最新鲜"，排序静默退化
        double ageHours = Math.max(0.0, (SimClock.now() - pub) / 3600000.0);
        return 1.0 / (1.0 + ageHours / 24.0);
    }
}
