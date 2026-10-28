package com.realtime.mock.generator;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {

    /** 关闭信号：钩子先置位，主循环看到后退出，避免“钩子在 flush、主循环还在发”造成尾部丢失。 */
    private static volatile boolean stopping = false;
    private static final AtomicBoolean finalized = new AtomicBoolean(false);

    public static void main(String[] args) {
        System.out.println("Starting RealTime Mock Generator...");
        System.out.println("[Config] bootstrap=" + AppConfig.BOOTSTRAP_SERVERS
                + " topic=" + AppConfig.TOPIC
                + " truthTopic=" + AppConfig.TRUTH_TOPIC
                + " strategy=" + AppConfig.DISTRIBUTION_STRATEGY
                + " costTier=" + AppConfig.COST_TIER
                + " loadTier=" + AppConfig.LOAD_TIER);
        System.out.println("[RunPlan] name=" + AppConfig.RUN_NAME
                + " durationSec=" + AppConfig.RUN_DURATION_SECONDS
                + " targetEventDays=" + AppConfig.RUN_TARGET_EVENT_DAYS
                + " maxEvents=" + AppConfig.RUN_MAX_EVENTS
                + " seed=" + AppConfig.RANDOM_SEED);
        System.out.println("[Chaos] enabled=" + AppConfig.CHAOS_ENABLED
                + " d1=" + AppConfig.CHAOS_D1_ENABLED + "/" + AppConfig.CHAOS_D1_RATE
                + " e1=" + AppConfig.CHAOS_E1_ENABLED + "/" + AppConfig.CHAOS_E1_RATE
                + " extended=" + AppConfig.CHAOS_EXTENDED_ENABLED + "/" + AppConfig.CHAOS_EXTENDED_RATE
                + " outOfOrder=" + AppConfig.CHAOS_OUT_OF_ORDER_ENABLED
                + "/" + AppConfig.CHAOS_OUT_OF_ORDER_RATE);
        System.out.println("[DataDefect] enabled=" + AppConfig.DATA_DEFECT_ENABLED
                + " rate=" + AppConfig.DATA_DEFECT_RATE
                + " duplicate=" + AppConfig.DATA_DEFECT_DUPLICATE_RATE
                + " drop=" + AppConfig.DATA_DEFECT_DROP_RATE
                + " modes=" + AppConfig.DATA_DEFECT_MODES);
        System.out.println("[Clock] 全局时钟＝模型活动之和（think=" + AppConfig.CLOCK_THINK_MIN_SECONDS + "-"
                + AppConfig.CLOCK_THINK_MAX_SECONDS + "s，停留 1:1）；并发会话上限=" + AppConfig.SESSION_MAX_OPEN
                + "；sessionTimeout=" + (AppConfig.sessionTimeoutMs() / 60000) + "min(事件时间)");

        MasterDataSeeder.seedIfMissing();
        SimClock.start(System.currentTimeMillis());

        DataManager dataManager = new DataManager();
        try {
            dataManager.load();
        } catch (Exception e) {
            System.err.println("Failed to load dimension data under: " + AppConfig.DATA_DIR);
            e.printStackTrace();
            return;
        }

        final MockGenerator generator = new MockGenerator(dataManager);
        final KafkaSender eventSender = new KafkaSender(AppConfig.TOPIC);
        final KafkaSender truthSender = new KafkaSender(AppConfig.TRUTH_TOPIC);
        final RunManifest manifest = new RunManifest();
        final FaultInjector faultInjector =
                new FaultInjector(new Random(AppConfig.derivedSeed(4)), manifest);
        final Random loopRandom = new Random(AppConfig.derivedSeed(6));
        final TrafficShaper shaper = new TrafficShaper(new Random(AppConfig.derivedSeed(5)));

        System.out.println("[RunManifest] runId=" + manifest.runId());
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> finish("shutdown_signal", generator, eventSender, truthSender, manifest)));

        final long wallStart = System.currentTimeMillis();
        final long clockStart = SimClock.peek();
        boolean pauseDone = false;
        String stopReason = null;

        try {
            while (!stopping) {
                long elapsedMs = System.currentTimeMillis() - wallStart;
                String limitReason = reachedLimit(elapsedMs, clockStart, manifest);
                if (limitReason != null) {
                    stopReason = limitReason;
                    break;
                }

                if (!pauseDone
                        && AppConfig.FAULT_PAUSE_AFTER_SECONDS > 0
                        && elapsedMs >= AppConfig.FAULT_PAUSE_AFTER_SECONDS * 1000L) {
                    pauseDone = true;
                    long pauseMs = Math.max(0, AppConfig.FAULT_PAUSE_DURATION_SECONDS) * 1000L;
                    manifest.injected("pause_seconds", pauseMs / 1000L);
                    if (pauseMs > 0) Thread.sleep(pauseMs);
                }

                int eps = shaper.nextEps(elapsedMs);
                long loopStart = System.currentTimeMillis();

                for (int i = 0; i < eps && !stopping; i++) {
                    GeneratedBatch batch = generator.generateNext();
                    manifest.addEvents(batch.events.size());
                    manifest.addTruths(batch.truths.size());

                    for (EventLog event : batch.events) {
                        List<FaultInjector.Payload> payloads = faultInjector.prepare(event);
                        for (FaultInjector.Payload payload : payloads) {
                            eventSender.sendRaw(payload.key, payload.value);
                        }
                    }
                    for (GroundTruth truth : batch.truths) truthSender.send(truth);

                    if (AppConfig.RUN_MAX_EVENTS > 0
                            && manifest.events() >= AppConfig.RUN_MAX_EVENTS) break;
                }

                Thread.sleep(5 + loopRandom.nextInt(16));
                long sleep = 1000 - (System.currentTimeMillis() - loopStart);
                if (sleep > 0) Thread.sleep(sleep);
                SimClock.persist();
            }
            if (stopReason == null) stopReason = "stopped";
        } catch (InterruptedException e) {
            stopReason = "interrupted";
            System.out.println("Generator stopped.");
        } catch (RuntimeException e) {
            stopReason = "uncaught_exception";
            throw e;
        } finally {
            finish(stopReason, generator, eventSender, truthSender, manifest);
        }
    }

    private static String reachedLimit(long elapsedMs, long clockStart, RunManifest manifest) {
        if (AppConfig.RUN_DURATION_SECONDS > 0
                && elapsedMs >= AppConfig.RUN_DURATION_SECONDS * 1000L) return "duration_seconds";
        if (AppConfig.RUN_TARGET_EVENT_DAYS > 0
                && SimClock.peek() - clockStart
                >= (long) (AppConfig.RUN_TARGET_EVENT_DAYS * 86_400_000.0)) return "target_event_days";
        if (AppConfig.RUN_MAX_EVENTS > 0
                && manifest.events() >= AppConfig.RUN_MAX_EVENTS) return "max_events";
        return null;
    }

    private static void finish(String reason, MockGenerator generator,
                               KafkaSender eventSender, KafkaSender truthSender,
                               RunManifest manifest) {
        if (!finalized.compareAndSet(false, true)) return;
        stopping = true;
        manifest.stop(reason == null ? "stopped" : reason);
        if ("shutdown_signal".equals(reason)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            eventSender.flush();
            truthSender.flush();
            eventSender.close();
            truthSender.close();
        } catch (Exception e) {
            System.err.println("[Shutdown] flush/close failed: " + e.getMessage());
        }
        SimClock.persist();
        try {
            generator.recordInjections(manifest);
            manifest.write();
        } catch (Exception e) {
            System.err.println("[Shutdown] manifest write failed: " + e.getMessage());
        }
        System.out.println("[Cost] " + generator.costStats());
    }
}
