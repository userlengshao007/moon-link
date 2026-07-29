package com.moon.im.constant;

/**
 * 单聊消息从持久化到推送完成过程中的状态值。
 */
public final class MessagePushStatus {

    public static final int SAVED = 0;
    public static final int PUSH_SUCCESS = 1;
    public static final int PUSH_FAILED = 2;

    private MessagePushStatus() {
    }
}
