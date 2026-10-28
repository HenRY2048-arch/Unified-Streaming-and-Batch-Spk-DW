package com.realtime.mock.generator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 维表与社交图种子生成（P1.1）。
 *
 * data.dir 下产出：
 *   dim_user_profile.csv  user_id|interest_tags|level|is_creator|primary_category|post_rate_level
 *   dim_item.csv          item_id|author_id|category|tags|content_len|published_ts
 *   dim_follow_seed.csv   follower|followee|ts      （有限有向图的 t0 种子，边表）
 *   .seed_version         种子格式版本（格式变更后自动重建）
 */
public class MasterDataSeeder {

    private static final String SEED_VERSION = "3";
    private static final ZoneId ZONE = ZoneId.of("GMT+8");
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static Map<String, String> TAG_TO_CATEGORY;

    public static void seedIfMissing() {
        try {
            File dir = new File(AppConfig.DATA_DIR);
            if (!dir.exists()) dir.mkdirs();

            File verFile = new File(dir, ".seed_version");
            if (verFile.exists() && SEED_VERSION.equals(readFirstLine(verFile))) {
                return;   // 版本一致，复用现有种子
            }

            Random rnd = new Random(AppConfig.derivedSeed(1));
            Map<String, String> tagToCategory = buildTagToCategory();

            List<User> users = buildUsers(rnd, tagToCategory);
            writeUsers(users);

            List<Item> items = buildItems(rnd, users);
            writeItems(items);

            int edges = writeFollowGraph(rnd, users, tagToCategory);

            writeText(verFile, SEED_VERSION);
            System.out.println("[MasterDataSeeder] seeded users=" + users.size()
                    + ", items=" + items.size() + ", followEdges=" + edges
                    + " (seed v" + SEED_VERSION + ") into " + AppConfig.DATA_DIR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ---------------- 用户 ----------------

    private static List<User> buildUsers(Random rnd, Map<String, String> tagToCategory) {
        List<User> users = new ArrayList<>(AppConfig.USER_COUNT);
        for (int i = 1; i <= AppConfig.USER_COUNT; i++) {
            users.add(sampleUser(100000L + i, rnd, tagToCategory));
        }
        return users;
    }

    /**
     * 按**与种子同一套分布**采样一个用户（P1.6）。
     * 运行期新用户注入复用本方法：属性分布必须一致，否则新老 cohort 行为会系统性不同，
     * U-01/U-03 会出现解释不清的偏差。
     */
    public static User sampleUser(long userId, Random rnd, Map<String, String> tagToCategory) {
        List<String> allTags = new ArrayList<>(tagToCategory.keySet());
        List<String> tags = new ArrayList<>();
        if (rnd.nextDouble() < AppConfig.TAG_COVERAGE) {   // 有画像；其余为冷启动
            int k = 1 + rnd.nextInt(3);
            List<String> pool = new ArrayList<>(allTags);
            Collections.shuffle(pool, rnd);
            tags.addAll(pool.subList(0, Math.min(k, pool.size())));
        }

        int level = sampleLevel(rnd);

        boolean creator = rnd.nextDouble() < AppConfig.CREATOR_RATIO;
        String primaryCategory = "";
        String postRateLevel = "";
        if (creator) {
            primaryCategory = pickPrimaryCategory(rnd, tags, tagToCategory);
            postRateLevel = pickPostRateLevel(rnd);
        }
        return new User(userId, tags, level, creator, primaryCategory, postRateLevel);
    }

    /** 标签 → 类目映射（缓存），供运行期注入复用。 */
    public static synchronized Map<String, String> tagToCategory() {
        if (TAG_TO_CATEGORY == null) TAG_TO_CATEGORY = buildTagToCategory();
        return TAG_TO_CATEGORY;
    }

    private static int sampleLevel(Random rnd) {
        double r = rnd.nextDouble();
        if (r < 0.05) return 1;
        if (r < 0.25) return 2;
        if (r < 0.75) return 3;
        if (r < 0.95) return 4;
        return 5;
    }

    private static String pickPrimaryCategory(Random rnd, List<String> tags, Map<String, String> tagToCategory) {
        if (!tags.isEmpty()) {
            String c = tagToCategory.get(tags.get(rnd.nextInt(tags.size())));
            if (c != null) return c;
        }
        return AppConfig.CATEGORY_TAGS[rnd.nextInt(AppConfig.CATEGORY_TAGS.length)][0];
    }

    private static String pickPostRateLevel(Random rnd) {
        double r = rnd.nextDouble();
        if (r < 0.20) return "HIGH";
        if (r < 0.70) return "MEDIUM";
        return "LOW";
    }

    // ---------------- 内容 ----------------

    private static List<Item> buildItems(Random rnd, List<User> users) {
        Map<String, List<Long>> creatorsByCategory = new HashMap<>();
        List<Long> allCreators = new ArrayList<>();
        for (User u : users) {
            if (u.isCreator()) {
                allCreators.add(u.getUserId());
                creatorsByCategory.computeIfAbsent(u.getPrimaryCategory(), k -> new ArrayList<>())
                        .add(u.getUserId());
            }
        }

        List<Item> items = new ArrayList<>(AppConfig.ITEM_INITIAL_COUNT);
        long itemId = 200001L;
        long now = System.currentTimeMillis();
        int perCategory = Math.max(1, AppConfig.ITEM_INITIAL_COUNT / AppConfig.CATEGORY_TAGS.length);

        for (String[] catTags : AppConfig.CATEGORY_TAGS) {
            String category = catTags[0];
            List<String> catTagList = Arrays.asList(catTags).subList(1, catTags.length);
            List<Long> authorPool = creatorsByCategory.getOrDefault(category, allCreators);

            for (int i = 0; i < perCategory; i++) {
                long authorId = authorPool.isEmpty()
                        ? users.get(rnd.nextInt(users.size())).getUserId()
                        : authorPool.get(rnd.nextInt(authorPool.size()));

                int k = 1 + rnd.nextInt(Math.min(3, catTagList.size()));
                List<String> pool = new ArrayList<>(catTagList);
                Collections.shuffle(pool, rnd);
                List<String> tags = new ArrayList<>(pool.subList(0, k));

                int contentLen = 50 + rnd.nextInt(4951);                     // [50, 5000]
                long ts = now - (long) rnd.nextInt(30 * 24 * 3600) * 1000L;  // 近 30 天内发布
                items.add(new Item(itemId++, authorId, category, tags, contentLen, formatTs(ts)));
            }
        }
        return items;
    }

    // ---------------- 社交图种子（分群 + 簇内优先连接 + 少量跨簇边）----------------

    private static int writeFollowGraph(Random rnd, List<User> users, Map<String, String> tagToCategory)
            throws IOException {
        Map<Long, String> cluster = new HashMap<>();
        Map<String, List<Long>> byCluster = new LinkedHashMap<>();
        Map<String, List<Long>> creatorsByCluster = new LinkedHashMap<>();

        for (User u : users) {
            String c;
            if (u.isCreator() && !u.getPrimaryCategory().isEmpty()) {
                c = u.getPrimaryCategory();
            } else if (!u.interestTags().isEmpty()) {
                c = tagToCategory.getOrDefault(u.interestTags().get(0), "COLD");
            } else {
                c = "COLD";
            }
            cluster.put(u.getUserId(), c);
            byCluster.computeIfAbsent(c, k -> new ArrayList<>()).add(u.getUserId());
            if (u.isCreator()) creatorsByCluster.computeIfAbsent(c, k -> new ArrayList<>()).add(u.getUserId());
        }

        Map<Long, Integer> inDeg = new HashMap<>();
        List<long[]> edges = new ArrayList<>();
        List<String> clusterKeys = new ArrayList<>(byCluster.keySet());
        long now = System.currentTimeMillis();

        for (User u : users) {
            long uId = u.getUserId();
            List<Long> same = byCluster.get(cluster.get(uId));
            Set<Long> chosen = new LinkedHashSet<>();
            int guard = 0;

            while (chosen.size() < AppConfig.GRAPH_AVG_OUTDEGREE
                    && guard++ < AppConfig.GRAPH_AVG_OUTDEGREE * 30) {
                List<Long> pool;
                double dice = rnd.nextDouble();
                List<Long> sameCreators = creatorsByCluster.get(cluster.get(uId));
                if (dice < 0.60 && sameCreators != null && !sameCreators.isEmpty()) {
                    pool = sameCreators;                       // 多关注"同簇创作者"（让关注流有内容可派）
                } else if (dice < (1.0 - AppConfig.GRAPH_CROSS_CLUSTER_RATIO)) {
                    pool = same;
                } else {
                    pool = byCluster.get(clusterKeys.get(rnd.nextInt(clusterKeys.size())));
                }
                if (pool == null || pool.isEmpty()) continue;

                long cand = weightedPick(rnd, pool, inDeg);
                if (cand == uId || chosen.contains(cand)) continue;

                chosen.add(cand);
                inDeg.merge(cand, 1, Integer::sum);
                edges.add(new long[]{uId, cand});
            }
        }

        try (PrintWriter writer = newWriter(AppConfig.FOLLOW_SEED_CSV)) {
            for (long[] e : edges) {
                long ts = now - (long) rnd.nextInt(60 * 24 * 3600) * 1000L;
                writer.println(e[0] + "|" + e[1] + "|" + formatTs(ts));
            }
        }
        return edges.size();
    }

    private static long weightedPick(Random rnd, List<Long> pool, Map<Long, Integer> inDeg) {
        long total = 0;
        for (long id : pool) total += inDeg.getOrDefault(id, 0) + 1;
        long r = (long) (rnd.nextDouble() * total);
        long acc = 0;
        for (long id : pool) {
            acc += inDeg.getOrDefault(id, 0) + 1;
            if (acc > r) return id;
        }
        return pool.get(pool.size() - 1);
    }

    // ---------------- 落盘 ----------------

    private static void writeUsers(List<User> users) throws IOException {
        try (PrintWriter writer = newWriter(AppConfig.USER_PROFILE_CSV)) {
            for (User u : users) {
                writer.println(u.getUserId() + "|" + String.join(",", u.interestTags()) + "|" + u.getLevel()
                        + "|" + u.isCreator() + "|" + u.getPrimaryCategory() + "|" + u.getPostRateLevel());
            }
        }
    }

    private static void writeItems(List<Item> items) throws IOException {
        try (PrintWriter writer = newWriter(AppConfig.ITEM_CSV)) {
            for (Item it : items) {
                writer.println(it.getItemId() + "|" + it.getAuthorId() + "|" + it.getCategory()
                        + "|" + String.join(",", it.getTags()) + "|" + it.getContentLen()
                        + "|" + it.getPublishedTs());
            }
        }
    }

    private static PrintWriter newWriter(String path) throws IOException {
        return new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8));
    }

    private static void writeText(File file, String text) throws IOException {
        try (PrintWriter w = newWriter(file.getPath())) {
            w.println(text);
        }
    }

    private static String readFirstLine(File file) {
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            return line == null ? "" : line.trim();
        } catch (IOException e) {
            return "";
        }
    }

    private static Map<String, String> buildTagToCategory() {
        Map<String, String> m = new LinkedHashMap<>();
        for (String[] catTags : AppConfig.CATEGORY_TAGS) {
            for (int i = 1; i < catTags.length; i++) m.put(catTags[i], catTags[0]);
        }
        return m;
    }

    private static String formatTs(long ms) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(ms), ZONE).format(TS_FMT);
    }
}
