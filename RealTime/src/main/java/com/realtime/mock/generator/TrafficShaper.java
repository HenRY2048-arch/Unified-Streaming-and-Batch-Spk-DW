package com.realtime.mock.generator;

import java.util.Random;

/**
 * 压力档位（C6 / E3）：负载档位决定每秒产生多少"生成单元"，**阈值全部来自配置**。
 *   steady：稳态
 *   peak  ：每 N 分钟洪峰 M 秒
 *   ramp  ：从 start 线性升到 end
 *   skew  ：低频 + 偶发大突发（测背压/热点）
 */
public class TrafficShaper {

    private final Random rnd;

    public TrafficShaper(Random rnd) {
        this.rnd = rnd;
    }

    public int nextEps(long elapsedMs) {
        switch (AppConfig.LOAD_TIER) {
            case "peak":
                return peak(elapsedMs);
            case "ramp":
                return ramp(elapsedMs);
            case "skew":
                return skew();
            default:
                return steady();
        }
    }

    private int steady() {
        return rand(AppConfig.EPS_STEADY_MIN, AppConfig.EPS_STEADY_MAX);
    }

    private int peak(long elapsedMs) {
        long periodMs = AppConfig.PEAK_INTERVAL_MINUTES * 60_000L;
        long durMs = AppConfig.PEAK_DURATION_SECONDS * 1000L;
        boolean isPeak = elapsedMs > periodMs && (elapsedMs % periodMs) < durMs;   // 第一个周期不冲
        return isPeak ? rand(AppConfig.EPS_PEAK_MIN, AppConfig.EPS_PEAK_MAX) : steady();
    }

    private int ramp(long elapsedMs) {
        double total = Math.max(1.0, AppConfig.RAMP_MINUTES * 60_000.0);
        double p = Math.min(1.0, elapsedMs / total);
        int base = (int) Math.round(AppConfig.EPS_RAMP_START
                + (AppConfig.EPS_RAMP_END - AppConfig.EPS_RAMP_START) * p);
        return Math.max(1, base + rnd.nextInt(11) - 5);
    }

    private int skew() {
        if (rnd.nextDouble() < AppConfig.EPS_SKEW_BURST_PROB) {
            return rand(AppConfig.EPS_SKEW_BURST_MIN, AppConfig.EPS_SKEW_BURST_MAX);
        }
        return rand(AppConfig.EPS_SKEW_BASE_MIN, AppConfig.EPS_SKEW_BASE_MAX);
    }

    private int rand(int min, int max) {
        if (max <= min) return min;
        return min + rnd.nextInt(max - min + 1);
    }
}
