package com.moon.link.cache;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 用户频道上下文映射管理器
 *
 * 用于维护 用户ID 与 Netty ChannelHandlerContext 的映射关系，
 * 支持在服务器端通过用户ID快速获取对应的网络连接上下文，
 * 实现消息推送、连接管理等功能。
 */
public class UserChannelCtxMap {
    /**
     * 存储用户ID与频道上下文的映射关系
     * Key: 用户ID (Long类型)
     * Value: Netty ChannelHandlerContext对象
     */
    private static final ConcurrentHashMap<Long, ChannelHandlerContext> CHANNEL_MAP = new ConcurrentHashMap<>();

    /**
     * 添加用户ID与频道上下文的映射关系
     *
     * @param userId 用户唯一标识ID
     * @param ctx    Netty频道上下文对象，用于与该用户进行通信
     */
    public static void add(Long userId, ChannelHandlerContext ctx) {
        CHANNEL_MAP.put(userId, ctx);
    }

    /**
     * 根据用户ID获取对应的频道上下文
     *
     * @param userId 用户唯一标识ID
     * @return 对应的Netty ChannelHandlerContext对象，如果用户不存在则返回null
     */
    public static ChannelHandlerContext get(Long userId) {
        return CHANNEL_MAP.get(userId);
    }

    public static boolean contains(Long userId) {
        return CHANNEL_MAP.containsKey(userId);
    }

    public static int size() {
        return CHANNEL_MAP.size();
    }

    /**
     * 移除指定用户ID的映射关系
     * 通常在用户断开连接或会话结束时调用，用于清理资源
     *
     * @param userId 用户唯一标识ID
     */
    public static void remove(Long userId) {
        CHANNEL_MAP.remove(userId);
    }
}
