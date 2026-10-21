package com.realtime.mock.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventLog {
    @JsonProperty("event_id")
    private String eventId;
    
    @JsonProperty("user_id")
    private Long userId;
    
    @JsonProperty("event_type")
    private String eventType;
    
    @JsonProperty("ts")
    private String ts;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    @JsonProperty("platform")
    private String platform;
    
    @JsonProperty("item_id")
    private Long itemId;
    
    @JsonProperty("target_user_id")
    private Long targetUserId;
    
    @JsonProperty("duration_sec")
    private Double durationSec;
    
    @JsonProperty("comment_text")
    private String commentText;
    
    @JsonProperty("content_len")
    private Integer contentLen;

    // ---- P1.2 新增字段（事件契约）----
    @JsonProperty("schema_version")
    private String schemaVersion;

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("position")
    private Integer position;

    @JsonProperty("source_item_id")
    private Long sourceItemId;

    @JsonProperty("source_user_id")
    private Long sourceUserId;

    @JsonProperty("author_id")
    private Long authorId;

    @JsonProperty("category")
    private String category;

    @JsonProperty("tags")
    private String tags;

    @JsonProperty("tag")
    private String tag;

    @JsonProperty("published_ts")
    private String publishedTs;

    @JsonProperty("items")
    private java.util.List<ExposureItem> items;

    // ---- P1.6 新增（契约 v2：`user_registered` 携带用户属性）----
    @JsonProperty("interest_tags")
    private java.util.List<String> interestTags;

    @JsonProperty("level")
    private Integer level;

    @JsonProperty("is_creator")
    private Boolean isCreator;

    @JsonProperty("primary_category")
    private String primaryCategory;

    @JsonProperty("post_rate_level")
    private String postRateLevel;

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getTs() { return ts; }
    public void setTs(String ts) { this.ts = ts; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getPlatform() { return platform; }
    public void setPlatform(String platform) { this.platform = platform; }

    public Long getItemId() { return itemId; }
    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Long getTargetUserId() { return targetUserId; }
    public void setTargetUserId(Long targetUserId) { this.targetUserId = targetUserId; }

    public Double getDurationSec() { return durationSec; }
    public void setDurationSec(Double durationSec) { this.durationSec = durationSec; }

    public String getCommentText() { return commentText; }
    public void setCommentText(String commentText) { this.commentText = commentText; }

    public Integer getContentLen() { return contentLen; }
    public void setContentLen(Integer contentLen) { this.contentLen = contentLen; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public Integer getPosition() { return position; }
    public void setPosition(Integer position) { this.position = position; }

    public Long getSourceItemId() { return sourceItemId; }
    public void setSourceItemId(Long sourceItemId) { this.sourceItemId = sourceItemId; }

    public Long getSourceUserId() { return sourceUserId; }
    public void setSourceUserId(Long sourceUserId) { this.sourceUserId = sourceUserId; }

    public Long getAuthorId() { return authorId; }
    public void setAuthorId(Long authorId) { this.authorId = authorId; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getTags() { return tags; }
    public void setTags(String tags) { this.tags = tags; }

    public String getTag() { return tag; }
    public void setTag(String tag) { this.tag = tag; }

    public String getPublishedTs() { return publishedTs; }
    public void setPublishedTs(String publishedTs) { this.publishedTs = publishedTs; }

    public java.util.List<ExposureItem> getItems() { return items; }
    public void setItems(java.util.List<ExposureItem> items) { this.items = items; }

    public java.util.List<String> getInterestTags() { return interestTags; }
    public void setInterestTags(java.util.List<String> interestTags) { this.interestTags = interestTags; }

    public Integer getLevel() { return level; }
    public void setLevel(Integer level) { this.level = level; }

    public Boolean getIsCreator() { return isCreator; }
    public void setIsCreator(Boolean isCreator) { this.isCreator = isCreator; }

    public String getPrimaryCategory() { return primaryCategory; }
    public void setPrimaryCategory(String primaryCategory) { this.primaryCategory = primaryCategory; }

    public String getPostRateLevel() { return postRateLevel; }
    public void setPostRateLevel(String postRateLevel) { this.postRateLevel = postRateLevel; }
}
