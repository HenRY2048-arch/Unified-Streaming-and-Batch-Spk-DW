package com.realtime.mock.generator;

import java.io.IOException;
import java.util.Random;

public class Main {

    /** 关闭信号：钩子先置位，主循环看到后退出，避免"钩子在 flush、主循环还在发"造成尾部丢失。 */
    private static volatile boolean stopping = false;

    public static void main(String[] args) {
        System.out.println("Starting RealTime Mock Generator...");
        System.out.println("[Config] bootstrap=" + AppConfig.BOOTSTRAP_SERVERS
                + " topic=" + AppConfig.TOPIC
                + " truthTopic=" + AppConfig.TRUTH_TOPIC
                + " strategy=" + AppConfig.DISTRIBUTION_STRATEGY
                + " costTier=" + AppConfig.COST_TIER
                + " loadTier=" + AppConfig.LOAD_TIER);
        System.out.println("[Clock] 全局时钟＝模型活动之和（think=" + AppConfig.CLOCK_THINK_MIN_SECONDS + "-"
                + AppConfig.CLOCK_THINK_MAX_SECONDS + "s，停留 1:1）；并发会话上限=" + AppConfig.SESSION_MAX_OPEN
                + "；sessionTimeout=" + (AppConfig.sessionTimeoutMs() / 60000) + "min(事件时间)");
        System.out.println("[Register] 新用户注入=" + AppConfig.USER_REGISTERED_PER_DAY
                + " 人/事件天, firstOpenDelay=" + AppConfig.USER_FIRST_OPEN_DELAY_MIN_MINUTES
                + "-" + AppConfig.USER_FIRST_OPEN_DELAY_MAX_MINUTES + " 事件分钟, idFrom="
                + AppConfig.USER_ID_INJECT_BASE + ", file=" + AppConfig.USER_INJECTED_CSV);

        MasterDataSeeder.seedIfMissing();
        // 事件时间起点（模拟时钟）
        SimClock.start(System.currentTimeMillis());

        DataManager dataManager = new DataManager();
        try {
            dataManager.load();
        } catch (IOException e) {
            System.err.println("Failed to load dimension data under: " + AppConfig.DATA_DIR);
            e.printStackTrace();
            return;
        }

        final MockGenerator generator = new MockGenerator(dataManager);
        KafkaSender eventSender = new KafkaSender(AppConfig.TOPIC);
        KafkaSender truthSender = new KafkaSender(AppConfig.TRUTH_TOPIC);
        Random random = new Random();
        TrafficShaper shaper = new TrafficShaper(random);

        // 收尾：flush 未发送完的事件 + 落盘事件时间 + 打印成本（C5 可观测）
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stopping = true;         // 让主循环先停
                Thread.sleep(200);       // 给它一点点时间退出当前这一批
                eventSender.flush();     // 被 kill 时 finally 不保证执行 → 必须在这里 flush，否则尾部事件会丢
                truthSender.flush();
                eventSender.close();
                truthSender.close();
            } catch (Exception ignore) {
                // 关闭阶段的异常不再抛出
            }
            SimClock.persist();          // 事件时间落盘：重启不倒退
            System.out.println("[Cost] " + generator.costStats());
        }));

        long startTime = System.currentTimeMillis();
        System.out.println("Generator is running. loadTier=" + AppConfig.LOAD_TIER);

        try {
            while (true) {
                if (stopping) break;
                int eps = shaper.nextEps(System.currentTimeMillis() - startTime);
                long loopStart = System.currentTimeMillis();

                for (int i = 0; i < eps; i++) {
                    GeneratedBatch batch = generator.generateNext();
                    for (EventLog event : batch.events) eventSender.send(event);
                    for (GroundTruth truth : batch.truths) truthSender.send(truth);
                }

                Thread.sleep(5 + random.nextInt(16));                     // 物理延迟
                long sleep = 1000 - (System.currentTimeMillis() - loopStart);
                if (sleep > 0) Thread.sleep(sleep);
                SimClock.persist();                                       // 每秒落盘（重启续跑）
            }
        } catch (InterruptedException e) {
            System.out.println("Generator stopped.");
        } finally {
            eventSender.close();
            truthSender.close();
        }
    }
}
