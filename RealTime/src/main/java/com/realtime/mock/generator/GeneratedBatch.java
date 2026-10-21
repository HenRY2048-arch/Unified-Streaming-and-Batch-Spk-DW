package com.realtime.mock.generator;

import java.util.List;

/** 一次生成同时产出：主事件流 + 归因真值（真值走独立 topic，不进事件流）。 */
public class GeneratedBatch {
    public final List<EventLog> events;
    public final List<GroundTruth> truths;

    public GeneratedBatch(List<EventLog> events, List<GroundTruth> truths) {
        this.events = events;
        this.truths = truths;
    }
}
