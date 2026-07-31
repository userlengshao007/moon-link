package com.moon.link.common.constant;

/**
 * Netty Channel 上保存的业务属性名称。
 */
public final class ChannelAttrKey {
    /** 已登录用户 ID，断开连接时据此清理用户状态。 */
    public static final String USER_ID = "userId";

    private ChannelAttrKey() {
    }
}
