package com.moon.link.common.constant;

/**
 * Netty Channel 上保存的业务属性名称。
 */
public final class ChannelAttrKey {
    /** 已登录用户 ID，断开连接时据此清理用户状态。 */
    public static final String USER_ID = "userId";
    /** 心跳累计次数，用于控制 Redis 在线状态的续期间隔。 */
    public static final String HEARTBEAT_TIMES = "heartBeatTimes";

    private ChannelAttrKey() {
    }
}
