package com.realtime.mock.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 归因**真值**（D2）。
 *
 * 记录每一次"转化"（互动 / 关注）的**真实来源路径**：它来自哪一次曝光的哪个位置、什么渠道、哪条内容。
 * **不进主事件流**（否则归因校验失去意义），走独立 Kafka topic，由数仓接成真值表做对账。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GroundTruth {
    @JsonProperty("truth_id")
    private String truthId;

    @JsonProperty("event_id")
    private String eventId;              // 对应的转化事件 id（可回连主事件）

    @JsonProperty("user_id")
    private Long userId;

    @JsonProperty("conversion_type")
    private String conversionType;       // like / fav / share / comment / follow

    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("target_user_id")
    private Long targetUserId;           // 关注时＝被关注者

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("position")
    private Integer position;

    @JsonProperty("session_id")
    private String sessionId;

    @JsonProperty("ts")
    private String ts;

    public String getTruthId() { return truthId; }
    public void setTruthId(String truthId) { this.truthId = truthId; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }

    public String getConversionType() { return conversionType; }
    public void setConversionType(String conversionType) { this.conversionType = conversionType; }

    public Long getItemId() { return itemId; }
    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Long getTargetUserId() { return targetUserId; }
    public void setTargetUserId(Long targetUserId) { this.targetUserId = targetUserId; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public Integer getPosition() { return position; }
    public void setPosition(Integer position) { this.position = position; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getTs() { return ts; }
    public void setTs(String ts) { this.ts = ts; }
}
