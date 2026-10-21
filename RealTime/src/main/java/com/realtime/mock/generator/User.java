package com.realtime.mock.generator;

import java.util.List;

public class User {
    private Long userId;
    private List<String> interestTags;
    private Integer level;
    private boolean creator;
    private String primaryCategory;
    private String postRateLevel;
    private String currentSessionId;
    private long lastActiveTime;
    /** 运行期活跃度（不落维表）：参与过的会话/事件计数，用于动态比例。 */
    private int activityScore;
    /** 来源：seed（t0 快照）/ registered（运行期注入）。 */
    private String source = "seed";
    /** 注册的**事件时间**（毫秒）；种子用户为 0（无注册事件）。 */
    private long registeredTs = 0L;
    /** 该用户从何时起（事件时间）可以进入会话旅途——注册 → 首次打开的间隔。 */
    private long activationSimMs = 0L;
    /**
     * 该用户自己的事件时钟（毫秒）：由"思考时间 + 停留时长"推进，事件时间戳取自它。
     * 这样 {@code leave_ts − enter_ts} 恒等于 {@code duration_sec}（不受全局时钟倍率影响）。
     */
    private long eventTimeMs = 0L;

    public User(Long userId, List<String> interestTags, Integer level,
                boolean creator, String primaryCategory, String postRateLevel) {
        this.userId = userId;
        this.interestTags = interestTags;
        this.level = level;
        this.creator = creator;
        this.primaryCategory = primaryCategory;
        this.postRateLevel = postRateLevel;
    }

    public Long getUserId() { return userId; }
    public List<String> interestTags() { return interestTags; }
    public Integer getLevel() { return level; }
    public boolean isCreator() { return creator; }
    public String getPrimaryCategory() { return primaryCategory; }
    public String getPostRateLevel() { return postRateLevel; }
    public String getCurrentSessionId() { return currentSessionId; }
    public void setCurrentSessionId(String currentSessionId) { this.currentSessionId = currentSessionId; }
    public long getLastActiveTime() { return lastActiveTime; }
    public void setLastActiveTime(long lastActiveTime) { this.lastActiveTime = lastActiveTime; }

    public int getActivityScore() { return activityScore; }
    public void bumpActivity() { this.activityScore++; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public long getRegisteredTs() { return registeredTs; }
    public void setRegisteredTs(long registeredTs) { this.registeredTs = registeredTs; }

    public long getActivationSimMs() { return activationSimMs; }
    public void setActivationSimMs(long activationSimMs) { this.activationSimMs = activationSimMs; }

    public long getEventTimeMs() { return eventTimeMs; }
    public void setEventTimeMs(long eventTimeMs) { this.eventTimeMs = eventTimeMs; }
    /** 推进本用户的事件时钟，返回推进后的值。 */
    public long advanceEventTime(long deltaMs) {
        if (deltaMs > 0) this.eventTimeMs += deltaMs;
        return this.eventTimeMs;
    }
}
