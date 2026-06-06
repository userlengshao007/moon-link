package com.moon.link.common.constant;

public class ChannelAttrKey {
    // 断开连接的时候知道这个 channel 属于哪个用户
    public static final String USER_ID = "userId";
    // 心跳累计几次后再续期 Redis
    public static final String HEARTBEAT_TIMES = "heartBeatTimes";
}
