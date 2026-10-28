package com.realtime.mock.generator;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生成器运行清单。
 *
 * <p>它回答一次观测运行中最基本的现场问题：用了什么配置、为什么停止、产出了多少事件和真值、
 * 各类异常/缺陷实际注入了多少次。清单与数据湖中的 Gate manifest 分离，表示“输入是怎么造出来的”，
 * 而不是“数仓结果是否可信”。
 */
public final class RunManifest {
    private static final DateTimeFormatter FILE_TS =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.of("GMT+8"));

    private final String runId;
    private final Instant startedAt;
    private final long clockStartMs;
    private final AtomicLong generatedEvents = new AtomicLong();
    private final AtomicLong generatedTruths = new AtomicLong();
    private final Map<String, AtomicLong> injections = new LinkedHashMap<>();
    private volatile String stopReason = "running";

    public RunManifest() {
        Instant now = Instant.now();
        this.runId = AppConfig.RUN_NAME + "-" + FILE_TS.format(now);
        this.startedAt = now;
        this.clockStartMs = SimClock.peek();
    }

    public String runId() {
        return runId;
    }

    public void addEvents(int count) {
        if (count > 0) generatedEvents.addAndGet(count);
    }

    public void addTruths(int count) {
        if (count > 0) generatedTruths.addAndGet(count);
    }

    public long events() {
        return generatedEvents.get();
    }

    public long truths() {
        return generatedTruths.get();
    }

    public void injected(String type, long count) {
        if (count <= 0) return;
        injections.computeIfAbsent(type, ignored -> new AtomicLong()).addAndGet(count);
    }

    public void stop(String reason) {
        if ("running".equals(stopReason)) stopReason = reason;
    }

    public synchronized File write() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("run_id", runId);
        data.put("run_name", AppConfig.RUN_NAME);
        data.put("started_at", startedAt.toString());
        data.put("ended_at", Instant.now().toString());
        data.put("stop_reason", stopReason);
        data.put("random_seed", AppConfig.RANDOM_SEED);
        data.put("configured_duration_seconds", AppConfig.RUN_DURATION_SECONDS);
        data.put("configured_target_event_days", AppConfig.RUN_TARGET_EVENT_DAYS);
        data.put("configured_max_events", AppConfig.RUN_MAX_EVENTS);
        data.put("generated_events", generatedEvents.get());
        data.put("generated_truths", generatedTruths.get());
        data.put("sim_clock_start_ms", clockStartMs);
        data.put("sim_clock_end_ms", SimClock.peek());
        data.put("sim_elapsed_seconds", (SimClock.peek() - clockStartMs) / 1000.0);
        data.put("load_tier", AppConfig.LOAD_TIER);
        data.put("distribution_strategy", AppConfig.DISTRIBUTION_STRATEGY);
        Map<String, Long> injectionSnapshot = new LinkedHashMap<>();
        injections.forEach((key, value) -> injectionSnapshot.put(key, value.get()));
        data.put("injections", injectionSnapshot);

        File dir = new File(AppConfig.RUN_MANIFEST_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("cannot create run manifest dir: " + dir);
        }
        File file = new File(dir, runId + ".json");
        try {
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file, data);
            System.out.println("[RunManifest] " + file.getPath());
            return file;
        } catch (IOException e) {
            throw new IllegalStateException("cannot write run manifest: " + file, e);
        }
    }
}
