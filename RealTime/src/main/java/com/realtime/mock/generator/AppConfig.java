package com.realtime.mock.generator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置中心（P1.0 外置）。
 * 读取顺序：-Dconfig.file=<path> 指定的外部文件 → classpath:config.properties → 内置默认值。
 */
public class AppConfig {

    /** 配置来源（须先于下面的静态常量初始化）。 */
    private static final Properties PROPS = load();

    // ---- Kafka ----
    public static final String BOOTSTRAP_SERVERS = get("kafka.bootstrap.servers", "localhost:9092");
    public static final String TOPIC = get("kafka.topic", "content_events");
    public static final String ACKS = get("kafka.acks", "1");

    // ---- 数据目录（原硬编码 /home/hyr/data_files）----
    public static final String DATA_DIR = get("data.dir", "data_files");
    public static final String USER_PROFILE_CSV = DATA_DIR + "/dim_user_profile.csv";
    public static final String ITEM_CSV = DATA_DIR + "/dim_item.csv";
    public static final String FOLLOW_SEED_CSV = DATA_DIR + "/dim_follow_seed.csv";
    /** 运行期注入的新用户（append；重启靠它避免重复发 id）。 */
    public static final String USER_INJECTED_CSV = get("mock.user.injected.file", DATA_DIR + "/dim_user_injected.csv");

    // ---- 模拟时钟（P1.6 / 契约 v2）----
    /** 会话内"思考时间"（事件秒），用于推进模拟时钟。 */
    public static final double CLOCK_THINK_MIN_SECONDS = Double.parseDouble(get("mock.clock.think.min.seconds", "2"));
    public static final double CLOCK_THINK_MAX_SECONDS = Double.parseDouble(get("mock.clock.think.max.seconds", "20"));
    /** 模拟时钟落盘文件（重启续跑用，避免事件时间回退）。 */
    public static final String SIM_CLOCK_FILE = get("mock.clock.file", DATA_DIR + "/.sim_clock");
    /** 是否跨重启续跑（true = 取 max(真实现在, 上次落盘值)）。 */
    public static final boolean SIM_CLOCK_RESUME = Boolean.parseBoolean(get("mock.clock.resume", "true"));

    // ---- 新用户注入（P1.6 / 契约 v2）----
    /** 新用户注入速率（**事件时间**口径：每"事件天"多少人）；0 = 关闭注入。 */
    public static final double USER_REGISTERED_PER_DAY = Double.parseDouble(get("mock.user.registered.per.day", "30"));
    /** 注册 → 首次打开的间隔（事件分钟），决定 U-03 的次留基线。 */
    public static final int USER_FIRST_OPEN_DELAY_MIN_MINUTES =
            Integer.parseInt(get("mock.user.first.open.delay.min.minutes", "0"));
    public static final int USER_FIRST_OPEN_DELAY_MAX_MINUTES =
            Integer.parseInt(get("mock.user.first.open.delay.max.minutes", "180"));
    /** 注入用户 id 的起始段（种子为 100001–101000，这里取高位段避免冲突）。 */
    public static final long USER_ID_INJECT_BASE = Long.parseLong(get("mock.user.id.inject.base", "300001"));

    // ---- 会话超时 ----
    public static final int SESSION_TIMEOUT_MINUTES = Integer.parseInt(get("mock.session.timeout.minutes", "30"));
    /** 已有会话的推进权重：越高越"深挖当前会话"，越低越"开新会话"（影响新用户/cohort 的样本厚度）。 */
    public static final double SESSION_RESUME_WEIGHT =
            Double.parseDouble(get("mock.session.resume.weight", "0.85"));
    /**
     * 并发会话数上限（模拟时钟的关键参数）。
     *
     * <p>为什么必须限制：全局事件时钟随"所有用户的模型活动之和"推进，会话则锚定它、并在会话内按自己的
     * 停留/思考推进。若并发会话太多，单个会话要很久才轮到下一步（实测 500 个并发 → 会话跨 150 真实秒），
     * 期间全局时钟已跑出几十小时，跨用户时间线一度回退 19 小时。限制并发后会话几个真实秒内走完，
     * 偏差被压在"一个会话的时长"量级（分钟级）。
     */
    public static final int SESSION_MAX_OPEN = Integer.parseInt(get("mock.session.max.open", "30"));

    /** 会话超时（事件毫秒）。注意：会话不因此切碎——主轴会话由"离线间隙"驱动，超时只影响创作者的发布会话。 */
    public static long sessionTimeoutMs() {
        return SESSION_TIMEOUT_MINUTES * 60_000L;
    }

    // ---- 规模 ----
    public static final int USER_COUNT = Integer.parseInt(get("mock.user.count", "1000"));
    public static final int ITEM_INITIAL_COUNT = Integer.parseInt(get("mock.item.initial.count", "500"));

    // ---- 兴趣 / 创作者 ----
    public static final double TAG_COVERAGE = Double.parseDouble(get("mock.tag.coverage", "0.8"));
    public static final double CREATOR_RATIO = Double.parseDouble(get("mock.creator.ratio", "0.15"));

    // ---- 社交图种子 ----
    public static final int GRAPH_AVG_OUTDEGREE = Integer.parseInt(get("mock.graph.avg.outdegree", "10"));
    public static final double GRAPH_CROSS_CLUSTER_RATIO =
            Double.parseDouble(get("mock.graph.cross.cluster.ratio", "0.08"));

    // ---- 事件契约（P1.2）----
    public static final String SCHEMA_VERSION = get("mock.schema.version", "v1");
    public static final int BATCH_SIZE = Integer.parseInt(get("mock.batch.size", "8"));
    public static final boolean EMIT_EXPOSURE = Boolean.parseBoolean(get("mock.emit.exposure", "true"));
    public static final boolean BATCH_INCLUDE_RANK_SCORE = Boolean.parseBoolean(get("mock.batch.rank.score", "true"));
    public static final double ITEM_PUBLISH_RATE = Double.parseDouble(get("mock.item.publish.rate", "0.005"));
    public static final double CHANNEL_WEIGHT_RECOMMEND = Double.parseDouble(get("mock.channel.weight.recommend", "0.45"));
    public static final double CHANNEL_WEIGHT_FOLLOW = Double.parseDouble(get("mock.channel.weight.follow", "0.35"));

    // ---- 会话旅途（P1.3）----
    public static final int SESSION_MAX_STEPS = Integer.parseInt(get("mock.session.max.steps", "12"));
    public static final double CLICK_BASE = Double.parseDouble(get("mock.click.base", "0.55"));
    public static final double CLICK_POSITION_DECAY = Double.parseDouble(get("mock.click.position.decay", "0.55"));
    public static final double CLICK_INTEREST_BOOST = Double.parseDouble(get("mock.click.interest.boost", "1.6"));
    public static final double DWELL_BASE_SECONDS = Double.parseDouble(get("mock.dwell.base.seconds", "2.0"));
    public static final double DWELL_CHARS_PER_SEC_MIN = Double.parseDouble(get("mock.dwell.chars.per.sec.min", "45"));
    public static final double DWELL_CHARS_PER_SEC_MAX = Double.parseDouble(get("mock.dwell.chars.per.sec.max", "110"));
    public static final double ENGAGE_LIKE = Double.parseDouble(get("mock.engage.like", "0.06"));
    public static final double ENGAGE_FAV = Double.parseDouble(get("mock.engage.fav", "0.03"));
    public static final double ENGAGE_SHARE = Double.parseDouble(get("mock.engage.share", "0.012"));
    public static final double ENGAGE_COMMENT = Double.parseDouble(get("mock.engage.comment", "0.006"));
    public static final double ENGAGE_DISLIKE = Double.parseDouble(get("mock.engage.dislike", "0.010"));
    public static final double FOLLOW_PROB = Double.parseDouble(get("mock.follow.prob", "0.02"));
    public static final double UNFOLLOW_PROB = Double.parseDouble(get("mock.unfollow.prob", "0.004"));

    // ---- 分发策略与成本（P1.4）----
    public static final String DISTRIBUTION_STRATEGY = get("mock.distribution.strategy", "hybrid");
    public static final String COST_TIER = get("mock.cost.tier", "standard");

    // ---- 压力档位（P1.4）----
    public static final String LOAD_TIER = get("mock.load.tier", "steady");
    public static final int EPS_STEADY_MIN = Integer.parseInt(get("mock.eps.steady.min", "30"));
    public static final int EPS_STEADY_MAX = Integer.parseInt(get("mock.eps.steady.max", "50"));
    public static final int EPS_PEAK_MIN = Integer.parseInt(get("mock.eps.peak.min", "150"));
    public static final int EPS_PEAK_MAX = Integer.parseInt(get("mock.eps.peak.max", "200"));
    public static final int PEAK_INTERVAL_MINUTES = Integer.parseInt(get("mock.peak.interval.minutes", "10"));
    public static final int PEAK_DURATION_SECONDS = Integer.parseInt(get("mock.peak.duration.seconds", "30"));
    public static final int EPS_RAMP_START = Integer.parseInt(get("mock.eps.ramp.start", "30"));
    public static final int EPS_RAMP_END = Integer.parseInt(get("mock.eps.ramp.end", "200"));
    public static final int RAMP_MINUTES = Integer.parseInt(get("mock.eps.ramp.minutes", "10"));
    public static final int EPS_SKEW_BASE_MIN = Integer.parseInt(get("mock.eps.skew.base.min", "20"));
    public static final int EPS_SKEW_BASE_MAX = Integer.parseInt(get("mock.eps.skew.base.max", "40"));
    public static final int EPS_SKEW_BURST_MIN = Integer.parseInt(get("mock.eps.skew.burst.min", "300"));
    public static final int EPS_SKEW_BURST_MAX = Integer.parseInt(get("mock.eps.skew.burst.max", "600"));
    public static final double EPS_SKEW_BURST_PROB = Double.parseDouble(get("mock.eps.skew.burst.prob", "0.02"));

    // ---- 归因真值（P1.4）----
    public static final String TRUTH_TOPIC = get("mock.truth.topic", "content_groundtruth");

    // Categories and Tags
    public static final String[][] CATEGORY_TAGS = {
        {"时尚美妆", "美妆", "穿搭"},
        {"生活记录", "旅行", "美食", "宠物萌宠"},
        {"知识成长", "科技", "学习"},
        {"身心健康", "健身运动", "情感"},
        {"家庭生活", "母婴育儿", "生活方式"}
    };

    // Corpus
    public static final String[] COMMENTS = {"写得真好", "学到了", "干货满满", "绝绝子", "收藏了", "踩坑了", "不太适合我"};
    public static final String[] DM_CONTENTS = {"怎么买？", "链接有吗？", "多少钱？", "求私信资料"};

    private static String get(String key, String defaultValue) {
        String sys = System.getProperty(key);          // 允许 -Dkey=value 单项覆盖（便于切压力/策略测试）
        if (sys != null && !sys.trim().isEmpty()) return sys.trim();
        String v = PROPS.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? defaultValue : v.trim();
    }

    private static Properties load() {
        Properties p = new Properties();

        String external = System.getProperty("config.file");
        if (external != null && !external.trim().isEmpty()) {
            try (InputStream in = new FileInputStream(external)) {
                p.load(in);
                System.out.println("[AppConfig] loaded external config: " + external);
                return p;
            } catch (IOException e) {
                System.err.println("[AppConfig] cannot read config.file=" + external
                        + " (" + e.getMessage() + "), fallback to classpath/defaults");
            }
        }

        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) {
                p.load(in);
                System.out.println("[AppConfig] loaded classpath:config.properties");
            }
        } catch (IOException e) {
            System.err.println("[AppConfig] classpath config.properties not loaded ("
                    + e.getMessage() + "), using defaults");
        }

        return p;
    }
}
