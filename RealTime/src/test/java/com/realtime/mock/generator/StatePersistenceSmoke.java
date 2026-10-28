package com.realtime.mock.generator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

/**
 * 无外部依赖的状态恢复冒烟测试。
 *
 * 直接运行：
 *   mvn -o -s settings.xml -B test-compile
 *   java -cp target/classes:target/test-classes com.realtime.mock.generator.StatePersistenceSmoke
 */
public final class StatePersistenceSmoke {

    private static final Path TEMP;

    static {
        try {
            String configured = System.getProperty("crt.test.data.dir");
            TEMP = configured == null || configured.trim().isEmpty()
                    ? Files.createTempDirectory("crt-state-smoke-")
                    : Path.of(configured);
            Files.createDirectories(TEMP);
            copySeeds(TEMP);
            // 显式传入 -Ddata.dir 时二者相同；本地直接运行也能落到独立临时目录。
            System.setProperty("data.dir", TEMP.toString());
            System.setProperty("mock.user.injected.file", TEMP.resolve("dim_user_injected.csv").toString());
            System.setProperty("mock.graph.state.file", TEMP.resolve("graph_state.log").toString());
            System.setProperty("mock.interaction.state.file", TEMP.resolve("interaction_state.log").toString());
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private StatePersistenceSmoke() {
    }

    public static void main(String[] args) throws Exception {
        testInteractionState(TEMP);
        testGraphState(TEMP);
        testChaosTruthAndUniqueness();
        testSessionPoolReuse();
        System.out.println("STATE_TEST_OK interaction=persisted graph_follow_unfollow=persisted "
                + "chaos_truth=complete session_pool=stable");
    }

    private static void copySeeds(Path target) throws IOException {
        Path source = Path.of(System.getProperty("crt.seed.source", "data_files"));
        for (String name : new String[]{
                "dim_user_profile.csv", "dim_item.csv", "dim_follow_seed.csv"}) {
            Files.copy(source.resolve(name), target.resolve(name), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void testInteractionState(Path temp) {
        Path file = temp.resolve("interaction_state.log");
        InteractionState first = new InteractionState(file.toString());
        first.markLike(1L, 2L);
        first.markDislike(1L, 2L); // 互斥：不得覆盖既有 LIKE
        first.markDislike(2L, 2L);
        first.markFaved(1L, 3L);

        require(first.stanceOf(1L, 2L) == InteractionState.Stance.LIKE,
                "like state missing");
        require(first.stanceOf(2L, 2L) == InteractionState.Stance.DISLIKE,
                "dislike state missing");
        require(first.isFaved(1L, 3L), "favorite state missing");

        InteractionState reloaded = new InteractionState(file.toString());
        require(reloaded.stanceOf(1L, 2L) == InteractionState.Stance.LIKE,
                "like state did not survive reload");
        require(reloaded.stanceOf(2L, 2L) == InteractionState.Stance.DISLIKE,
                "dislike state did not survive reload");
        require(reloaded.isFaved(1L, 3L), "favorite state did not survive reload");
    }

    private static void testGraphState(Path temp) throws IOException {
        DataManager first = new DataManager();
        first.load();

        long follower = -1L;
        long followee = -1L;
        outer:
        for (User u : first.getUsers()) {
            for (User v : first.getUsers()) {
                if (!u.getUserId().equals(v.getUserId())
                        && !first.isFollowing(u.getUserId(), v.getUserId())) {
                    follower = u.getUserId();
                    followee = v.getUserId();
                    break outer;
                }
            }
        }
        require(follower >= 0 && followee >= 0, "could not find an unused edge");

        first.addFollow(follower, followee);
        require(first.isFollowing(follower, followee), "follow did not update memory graph");
        first.removeFollow(follower, followee);

        DataManager reloaded = new DataManager();
        reloaded.load();
        require(!reloaded.isFollowing(follower, followee),
                "follow/unfollow journal did not replay correctly");
        require(Files.exists(temp.resolve("graph_state.log")), "graph state journal was not created");
    }

    private static void testChaosTruthAndUniqueness() throws IOException {
        DataManager data = new DataManager();
        data.load();
        MockGenerator generator = new MockGenerator(data);
        GeneratedBatch burst = null;
        for (int i = 0; i < 500; i++) {
            GeneratedBatch current = generator.generateNext();
            if (current.events.size() == 15
                    && current.events.stream().allMatch(e -> "like".equals(e.getEventType()))) {
                burst = current;
            }
        }
        require(burst != null, "A2 burst was not reached");
        require(burst.truths.size() == 15, "A2 truth count mismatch");
        Set<Long> itemIds = new HashSet<>();
        for (EventLog event : burst.events) {
            itemIds.add(event.getItemId());
            require("v2".equals(event.getSchemaVersion()), "event schema_version is not v2");
        }
        require(itemIds.size() == 15, "A2 likes are not on 15 distinct items");
        require(burst.truths.stream().allMatch(t -> "like".equals(t.getConversionType())),
                "A2 truth conversion type mismatch");
    }

    /**
     * 回归会话池空槽：当结束的会话恰好在 activeUsers 末尾时，旧实现会留下 stale 索引，
     * 后续 remove 会越界。这里连续推进足够多步，覆盖会话反复打开、结束和重排。
     */
    private static void testSessionPoolReuse() throws IOException {
        DataManager data = new DataManager();
        data.load();
        MockGenerator generator = new MockGenerator(data);
        for (int i = 0; i < 20_000; i++) {
            generator.generateNext();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
