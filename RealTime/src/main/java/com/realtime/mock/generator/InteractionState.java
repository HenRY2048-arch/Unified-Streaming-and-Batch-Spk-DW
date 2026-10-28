package com.realtime.mock.generator;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    private final String stateFile;

    public InteractionState(String stateFile) {
        this.stateFile = stateFile;
        load();
    }

    public Stance stanceOf(long userId, long itemId) {
        if (contains(liked, userId, itemId)) return Stance.LIKE;
        if (contains(disliked, userId, itemId)) return Stance.DISLIKE;
        return Stance.NONE;
    }

    public synchronized void markLike(long userId, long itemId) {
        if (contains(disliked, userId, itemId)) return;
        liked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
        append("L", userId, itemId);
    }

    public synchronized void markDislike(long userId, long itemId) {
        if (contains(liked, userId, itemId)) return;
        disliked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
        append("D", userId, itemId);
    }

    public boolean isFaved(long userId, long itemId) { return contains(faved, userId, itemId); }

    public synchronized void markFaved(long userId, long itemId) {
        if (contains(faved, userId, itemId)) return;
        faved.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
        append("F", userId, itemId);
    }

    private static boolean contains(Map<Long, Set<Long>> m, long userId, long itemId) {
        Set<Long> s = m.get(userId);
        return s != null && s.contains(itemId);
    }

    private synchronized void load() {
        File file = new File(stateFile);
        if (!file.exists()) return;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                String[] p = line.split("\\|", -1);
                if (p.length != 3) continue;
                long userId = Long.parseLong(p[1]);
                long itemId = Long.parseLong(p[2]);
                switch (p[0]) {
                    case "L":
                        if (contains(disliked, userId, itemId)) break;
                        liked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
                        break;
                    case "D":
                        if (contains(liked, userId, itemId)) break;
                        disliked.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
                        break;
                    case "F":
                        faved.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
                        break;
                    default:
                        break;
                }
            }
            System.out.println("[InteractionState] loaded state: " + stateFile);
        } catch (IOException | NumberFormatException e) {
            System.err.println("[InteractionState] load failed: " + e.getMessage());
        }
    }

    private synchronized void append(String kind, long userId, long itemId) {
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(stateFile, true), StandardCharsets.UTF_8))) {
            w.write(kind + "|" + userId + "|" + itemId);
            w.newLine();
        } catch (IOException e) {
            System.err.println("[InteractionState] append failed: " + e.getMessage());
        }
    }
}
