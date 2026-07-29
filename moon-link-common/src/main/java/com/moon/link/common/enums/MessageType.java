package com.moon.link.common.enums;

/**
 * 协议头中的业务消息类型。
 */
public enum MessageType {
    /** 登录请求或响应。 */
    LOGIN_MESSAGE((short) 0),
    /** 客户端心跳或服务端 pong。 */
    HEARTBEAT_MESSAGE((short) 1),
    /** 消息处理确认。 */
    ACK_MESSAGE((short) 2),
    /** 强制客户端下线。 */
    FORCE_OFFLINE_MESSAGE((short) 3),
    /** 单聊消息。 */
    PRIVATE_CHAT_MESSAGE((short) 4),
    /** 群聊消息。 */
    GROUP_CHAT_MESSAGE((short) 5),
    /** 系统通知消息。 */
    SYSTEM_MESSAGE((short) 6);

    /** 写入协议头的整数编码。 */
    private final int type;

    MessageType(int type) {
        this.type = type;
    }

    /**
     * 获取协议编码。
     *
     * @return 消息类型编码
     */
    public int getType() {
        return type;
    }

    /**
     * 将协议编码转换为消息类型。
     *
     * @param type 协议编码
     * @return 对应类型；编码未知时返回 {@code null}
     */
    public static MessageType fromType(int type) {
        for (MessageType value : values()) {
            if (value.type == type) {
                return value;
            }
        }
        return null;
    }
}
