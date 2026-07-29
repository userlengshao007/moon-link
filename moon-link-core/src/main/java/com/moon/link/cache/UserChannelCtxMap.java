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
public final class UserChannelCtxMap {
    /**
     * 存储用户ID与频道上下文的映射关系
     * Key: 用户ID (Long类型)
     * Value: Netty ChannelHandlerContext对象
     */
    private static final ConcurrentHashMap<Long, ChannelHandlerContext> CHANNEL_MAP = new ConcurrentHashMap<>();

    /**
     * 原子替换用户当前连接。
     *
     * @param userId 用户唯一标识 ID
     * @param newContext 用户的新连接上下文
     * @return 被替换的旧连接；首次登录时返回 {@code null}
     */
    public static ChannelHandlerContext replace(Long userId, ChannelHandlerContext newContext) {
        return CHANNEL_MAP.put(userId, newContext);
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

    /**
     * 判断用户是否连接到当前节点。
     *
     * @param userId 用户 ID
     * @return 本地是否存在该用户连接
     */
    public static boolean contains(Long userId) {
        return CHANNEL_MAP.containsKey(userId);
    }

    /**
     * 获取当前节点维护的用户连接数。
     *
     * @return 本地连接数
     */
    public static int size() {
        return CHANNEL_MAP.size();
    }

    /**
     * 仅当用户当前连接仍是预期连接时移除映射。
     *
     * @param userId 用户唯一标识 ID
     * @param expectedContext 触发断开的连接上下文
     * @return 是否成功移除映射
     */
    public static boolean remove(Long userId, ChannelHandlerContext expectedContext) {
        return CHANNEL_MAP.remove(userId, expectedContext);
    }

    private UserChannelCtxMap() {
    }
}
