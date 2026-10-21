package com.realtime.mock.generator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 维表与社交图种子的加载（P1.1）。
 * 解析 MasterDataSeeder 产出的文件；社交图以邻接表载入内存（供后续分发使用）。
 */
public class DataManager {
    private final List<User> users = new ArrayList<>();
    private final Map<Long, User> usersById = new HashMap<>();
    private final List<Item> items = new ArrayList<>();
    private final Map<String, List<Item>> categoryToItems = new HashMap<>();
    private final Map<String, String> tagToCategory = new LinkedHashMap<>();
    private final Map<Long, Set<Long>> following = new HashMap<>();   // follower -> followees
    private final Map<Long, Set<Long>> followers = new HashMap<>();   // followee -> followers
    private final Map<Long, Item> itemById = new HashMap<>();
    private final Map<Long, List<Item>> itemsByAuthor = new HashMap<>();
    private final Map<String, List<Item>> itemsByTag = new HashMap<>();
    private final List<User> creators = new ArrayList<>();
    private long nextItemId = 200001L;
    private final Random random = new Random();

    /** 已"上线"用户（种子全部立即上线；注入用户等 activationSimMs 到点，模拟"注册→首次打开"）。 */
    private final List<User> activeUsers = new ArrayList<>();
    private final List<User> pendingUsers = new ArrayList<>();
    private final List<User> activeCreators = new ArrayList<>();
    private long nextInjectUserId = AppConfig.USER_ID_INJECT_BASE;

    public void load() throws IOException {
        buildTagToCategory();
        loadUsers();
        loadInjectedUsers();
        loadItems();
        loadFollowSeed();
        activateLoadedUsers();
    }

    /** 把已加载用户分为"已上线/待激活"，并把注入 id 计数器抬到现有最大 id 之上。 */
    private void activateLoadedUsers() {
        long simNow = SimClock.peek();
        for (User u : users) {
            if (u.getActivationSimMs() <= simNow) {
                activeUsers.add(u);
                if (u.isCreator()) activeCreators.add(u);
            } else {
                pendingUsers.add(u);
            }
            if (u.getUserId() >= nextInjectUserId) nextInjectUserId = u.getUserId() + 1;
        }
    }

    /**
     * 载入历史注入用户（P1.6）：与种子同样的 6 列格式。
     * 作用：生成器重启后**不会重复发同一个 user_id**（否则会伪造出一批假"新用户"）。
     */
    private void loadInjectedUsers() {
        File f = new File(AppConfig.USER_INJECTED_CSV);
        if (!f.exists()) return;
        try {
            for (String line : readLines(AppConfig.USER_INJECTED_CSV)) {
                String[] p = line.split("\\|", -1);
                if (p.length < 6) continue;
                long id = Long.parseLong(p[0]);
                if (usersById.containsKey(id)) continue;
                List<String> tags = p[1].isEmpty()
                        ? new ArrayList<>() : new ArrayList<>(Arrays.asList(p[1].split(",")));
                User u = new User(id, tags, Integer.parseInt(p[2]),
                        Boolean.parseBoolean(p[3]), p[4], p[5]);
                u.setSource("registered");
                users.add(u);
                usersById.put(id, u);
                if (u.isCreator()) creators.add(u);
            }
            System.out.println("[DataManager] 已载入历史注入用户：" + f.getPath());
        } catch (IOException e) {
            System.err.println("[DataManager] 载入注入用户失败: " + e.getMessage());
        }
    }

    private void buildTagToCategory() {
        for (String[] catTags : AppConfig.CATEGORY_TAGS) {
            for (int i = 1; i < catTags.length; i++) tagToCategory.put(catTags[i], catTags[0]);
        }
    }

    private void loadUsers() throws IOException {
        for (String line : readLines(AppConfig.USER_PROFILE_CSV)) {
            String[] p = line.split("\\|", -1);
            if (p.length < 6) continue;
            List<String> tags = p[1].isEmpty()
                    ? new ArrayList<>() : new ArrayList<>(Arrays.asList(p[1].split(",")));
            User user = new User(Long.parseLong(p[0]), tags, Integer.parseInt(p[2]),
                    Boolean.parseBoolean(p[3]), p[4], p[5]);
            users.add(user);
            usersById.put(user.getUserId(), user);
            if (user.isCreator()) creators.add(user);
        }
    }

    private void loadItems() throws IOException {
        for (String line : readLines(AppConfig.ITEM_CSV)) {
            String[] p = line.split("\\|", -1);
            if (p.length < 6) continue;
            List<String> tags = p[3].isEmpty()
                    ? new ArrayList<>() : new ArrayList<>(Arrays.asList(p[3].split(",")));
            Item item = new Item(Long.parseLong(p[0]), Long.parseLong(p[1]), p[2],
                    tags, Integer.parseInt(p[4]), p[5]);
            items.add(item);
            indexItem(item);
        }
    }

    private void indexItem(Item item) {
        categoryToItems.computeIfAbsent(item.getCategory(), k -> new ArrayList<>()).add(item);
        itemById.put(item.getItemId(), item);
        itemsByAuthor.computeIfAbsent(item.getAuthorId(), k -> new ArrayList<>()).add(item);
        for (String t : item.getTags()) itemsByTag.computeIfAbsent(t, k -> new ArrayList<>()).add(item);
        if (item.getItemId() >= nextItemId) nextItemId = item.getItemId() + 1;
    }

    private void loadFollowSeed() throws IOException {
        for (String line : readLines(AppConfig.FOLLOW_SEED_CSV)) {
            String[] p = line.split("\\|", -1);
            if (p.length < 2) continue;
            long follower = Long.parseLong(p[0]);
            long followee = Long.parseLong(p[1]);
            following.computeIfAbsent(follower, k -> new HashSet<>()).add(followee);
            followers.computeIfAbsent(followee, k -> new HashSet<>()).add(follower);
        }
    }

    private static List<String> readLines(String path) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.trim().isEmpty()) lines.add(line);
            }
        }
        return lines;
    }

    // ---------------- 访问 ----------------

    public User getRandomUser() {
        refreshActive();
        return activeUsers.get(random.nextInt(activeUsers.size()));
    }

    /** 待激活用户到点即上线（"注册 → 首次打开"的间隔）。 */
    private void refreshActive() {
        if (pendingUsers.isEmpty()) return;
        long simNow = SimClock.peek();
        Iterator<User> it = pendingUsers.iterator();
        while (it.hasNext()) {
            User u = it.next();
            if (u.getActivationSimMs() <= simNow) {
                activeUsers.add(u);
                if (u.isCreator()) activeCreators.add(u);
                it.remove();
            }
        }
    }

    public User getUser(long userId) { return usersById.get(userId); }

    /** 随机取一个创作者（供给侧的产出者）。 */
    public User getRandomCreator() {
        refreshActive();
        if (activeCreators.isEmpty()) return getRandomUser();
        return activeCreators.get(random.nextInt(activeCreators.size()));
    }

    public Item getRandomItem() { return items.get(random.nextInt(items.size())); }

    public List<User> getUsers() { return users; }

    public List<Item> getItems() { return items; }

    public Set<Long> getFollowing(long userId) { return following.getOrDefault(userId, Collections.emptySet()); }

    public Set<Long> getFollowers(long userId) { return followers.getOrDefault(userId, Collections.emptySet()); }

    public boolean isFollowing(long follower, long followee) {
        Set<Long> s = following.get(follower);
        return s != null && s.contains(followee);
    }

    /** 关注：真正改图（边由旅途产生）。 */
    public void addFollow(long follower, long followee) {
        following.computeIfAbsent(follower, k -> new HashSet<>()).add(followee);
        followers.computeIfAbsent(followee, k -> new HashSet<>()).add(follower);
    }

    public void removeFollow(long follower, long followee) {
        Set<Long> s = following.get(follower);
        if (s != null) s.remove(followee);
        Set<Long> t = followers.get(followee);
        if (t != null) t.remove(follower);
    }

    public int followerCount(long userId) { return followers.getOrDefault(userId, Collections.emptySet()).size(); }

    public Item getItem(long itemId) { return itemById.get(itemId); }

    public List<Item> getItemsByAuthor(long authorId) {
        return itemsByAuthor.getOrDefault(authorId, Collections.emptyList());
    }

    public List<Item> getItemsByTag(String tag) {
        return itemsByTag.getOrDefault(tag, Collections.emptyList());
    }

    public synchronized long nextItemId() { return nextItemId++; }

    /** 作者产出新内容时调用：既入内存池，也进各索引。 */
    public synchronized void addItem(Item item) {
        items.add(item);
        indexItem(item);
    }

    // ---------------- 新用户注入（P1.6 / 契约 v2） ----------------

    /** 分配下一个注入用户 id（种子 100001–101000 之外的高位段）。 */
    public synchronized long nextInjectUserId() {
        return nextInjectUserId++;
    }

    /** 运行期注入一个新用户：入内存池（待激活）并 **append 落盘**。 */
    public synchronized void registerUser(User user) {
        if (usersById.containsKey(user.getUserId())) return;
        users.add(user);
        usersById.put(user.getUserId(), user);
        if (user.isCreator()) creators.add(user);
        if (user.getActivationSimMs() <= SimClock.peek()) {
            activeUsers.add(user);
            if (user.isCreator()) activeCreators.add(user);
        } else {
            pendingUsers.add(user);
        }
        if (user.getUserId() >= nextInjectUserId) nextInjectUserId = user.getUserId() + 1;
        appendInjectedUser(user);
    }

    private void appendInjectedUser(User user) {
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(AppConfig.USER_INJECTED_CSV, true), StandardCharsets.UTF_8))) {
            w.write(user.getUserId() + "|" + String.join(",", user.interestTags()) + "|"
                    + user.getLevel() + "|" + user.isCreator() + "|"
                    + nz(user.getPrimaryCategory()) + "|" + nz(user.getPostRateLevel()));
            w.newLine();
        } catch (IOException e) {
            System.err.println("[DataManager] 写入注入用户失败: " + e.getMessage());
        }
    }

    private static String nz(String s) { return s == null ? "" : s; }

    /**
     * 兼容旧行为：标签 → 类目 → 该类目随机取一篇；无标签则全量随机。
     * 真正的分发控制器在 P1.3 重造，本步不动行为。
     */
    public Item getRecommendItem(User user) {
        if (random.nextDouble() < 0.7 && !user.interestTags().isEmpty()) {
            String tag = user.interestTags().get(random.nextInt(user.interestTags().size()));
            String category = tagToCategory.get(tag);
            List<Item> pool = category == null ? null : categoryToItems.get(category);
            if (pool != null && !pool.isEmpty()) return pool.get(random.nextInt(pool.size()));
        }
        return getRandomItem();
    }
}
