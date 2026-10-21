package com.realtime.mock.generator;

import java.util.*;

/**
 * 互动状态（源头修复："一次性 / 针对性"）。
 *
 *  - **状态型**（对 (user,item) 一次性）：`like` / `dislike`（**二者互斥**）、`fav`；
 *  - **动作型**（可重复）：`comment` / `share` / `dm` / 曝光 / 进入 / 进主页 / 点标签；
 *  - **不做撤回**（无 `unlike` / `unfav`）→ 状态**单调**，因此"某时刻的点赞数"可由事件流**累计**得出。
 *
 * 于是统计口径变成良定义：
 *  - 帖子累计点赞数 ＝ 截至该时刻的 `like` 事件数 ＝ **distinct 点赞用户数**（因去重）；
 *  - 窗口新增点赞数 ＝ 窗口内 `like` 事件数（不重叠窗口可加和）；
 *  - 用户正/负反馈 ＝ 其 `stance` 聚合。
 */
public class InteractionState {

    public enum Stance { NONE, LIKE, DISLIKE }

    private final Map<Long, Set<Long>> liked = new HashMap<>();
    private final Map<Long, Set<Long>> disliked = new HashMap<>();
    private final Map<Long, Set<Long>> faved = new HashMap<>();

    public Stance stanceOf(long userId, long itemId) {
        if (contains(liked, userId, itemId)) return Stance.LIKE;
        if (contains(disliked, userId, itemId)) return Stance.DISLIKE;
        return Stance.NONE;
    }

    public void markLike(long userId, long itemId) {
        liked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
    }

    public void markDislike(long userId, long itemId) {
        disliked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
    }

    public boolean isFaved(long userId, long itemId) { return contains(faved, userId, itemId); }

    public void markFaved(long userId, long itemId) {
        faved.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
    }

    private static boolean contains(Map<Long, Set<Long>> m, long userId, long itemId) {
        Set<Long> s = m.get(userId);
        return s != null && s.contains(itemId);
    }
}
