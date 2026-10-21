package com.realtime.mock.generator;

/**
 * 模拟时钟（P1.6 / 契约 v2）。
 *
 * <p>为什么需要它：原来所有事件时间都取自 {@code System.currentTimeMillis()}，而 {@code duration_sec}
 * 是停留模型的采样值——同一份数据里存在两套互不推导的时间（实测：enter→leave 的 ts 差中位 0.965s，
 * 而 duration_sec 中位 24.03s）。于是会话时长/形态失真，且"发布后 24h""次日留存""关注后 N 天"
 * 这类跨天窗口在本地根本跑不出来。
 *
 * <p>三条不变量（都踩过坑，别再改回去）：
 * <ol>
 *   <li><b>事件时间戳取自用户自己的时钟</b>（{@code User.eventTimeMs}）→ {@code leave_ts − enter_ts}
 *       恒等于 {@code duration_sec}；若取全局时钟，别人的停留会插进 enter↔leave 之间污染差值。</li>
 *   <li><b>全局时钟 ＝ 模型活动之和</b>（{@link #advanceModel}），不绑真实时间，因此进度与机器速度、
 *       EPS 节奏无关，可复现。会话开始时用户时钟**锚定**它（{@code MockGenerator.startSession}），
 *       会话内只按自己的停留/思考推进。</li>
 *   <li><b>并发会话必须限流</b>（{@code mock.session.max.open}）：并发越多，单个会话越久才轮到下一步，
 *       期间全局时钟已跑出几十小时 → 跨用户时间线回退。实测 500 并发时回退达 19 小时；限流后压到分钟级。</li>
 * </ol>
 *
 * <p>跨天速度：每真实秒推进 ≈「每秒步数 × 平均模型时长」，例如 85 EPS 时约 10 分钟事件时间/真实秒
 * （即 1 个事件天 ≈ 2~3 真实分钟）。慢机器 / 低 EPS 会等比变慢，但轨迹与倍率无关。
 *
 * <p>事件时间会落盘（{@code mock.clock.file}），重启续跑，**不倒退**。
 */
public final class SimClock {

    private static final java.io.File PERSIST_FILE = new java.io.File(AppConfig.SIM_CLOCK_FILE);

    private static long simMs = 0L;

    private SimClock() {
    }

    /**
     * 由入口（Main）在生成之前调用一次，锚定事件时间起点。
     *
     * <p>起点取 {@code max(真实现在, 上次持久化的事件时间)}：事件时间可能被加速推到"未来"，
     * 重启若直接回到真实现在会造成**事件时间回退**（最多可退几天），
     * 使下游 ODS 的分区/窗口"隔天倒灌"。因此默认续跑（可关，见 {@code mock.clock.resume}）。
     */
    public static synchronized long start(long epochMs) {
        long persisted = readPersisted();
        simMs = AppConfig.SIM_CLOCK_RESUME ? Math.max(epochMs, persisted) : epochMs;
        return simMs;
    }

    /** 当前事件时间（只读）。 */
    public static synchronized long now() {
        return simMs;
    }

    /** 同 {@link #now()}。 */
    public static synchronized long peek() {
        return simMs;
    }

    /**
     * 模型推进：把一段模型时长（停留 / 思考）计入全局事件时间。
     * 这是事件时间**唯一**的前进来源——全局时钟与所有用户的时间线同源，不会因真实时间流逝而分叉。
     */
    public static synchronized long advanceModel(long modelMs) {
        if (modelMs > 0) simMs += modelMs;
        return simMs;
    }

    /** 观测一个用户的时间：全局时钟只进不退（用户会话内可能略领先）。 */
    public static synchronized long observe(long eventMs) {
        if (eventMs > simMs) simMs = eventMs;
        return simMs;
    }

    /** 把当前事件时间落盘（供重启续跑）。 */
    public static synchronized void persist() {
        try (java.io.Writer w = new java.io.OutputStreamWriter(
                new java.io.FileOutputStream(PERSIST_FILE, false), java.nio.charset.StandardCharsets.UTF_8)) {
            w.write(Long.toString(simMs));
        } catch (java.io.IOException e) {
            // 落盘失败不影响生成：下次仍可用真实时间兜底
        }
    }

    private static long readPersisted() {
        try {
            if (!PERSIST_FILE.exists()) return 0L;
            try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(
                    new java.io.FileInputStream(PERSIST_FILE), java.nio.charset.StandardCharsets.UTF_8))) {
                String line = r.readLine();
                return line == null ? 0L : Long.parseLong(line.trim());
            }
        } catch (Exception e) {
            return 0L;
        }
    }
}
