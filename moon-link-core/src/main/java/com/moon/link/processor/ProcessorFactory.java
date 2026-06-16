package com.moon.link.processor;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.enums.MessageType;

/**
 * 消息处理器工厂类
 * 根据消息类型获取对应的消息处理器实例，采用单例模式管理处理器对象，
 * 确保每种消息类型在系统中只有一个处理器实例。
 */
public class ProcessorFactory {
    /** 登录消息处理器单例 */
    private static final LoginProcessor LOGIN_PROCESSOR = new LoginProcessor();
    
    /** 心跳消息处理器单例 */
    private static final HeartBeatProcessor HEART_BEAT_PROCESSOR = new HeartBeatProcessor();

    /** 私聊消息处理器单例 */
    private static final PrivateChatProcessor PRIVATE_CHAT_PROCESSOR = new PrivateChatProcessor();

    /**
     * 根据消息类型获取对应的消息处理器
     *
     * @param messageType 消息类型枚举，用于确定需要哪种处理器
     * @return 对应的消息处理器实例，类型为 AbstractMessageProcessor&lt;CompleteMessage&gt;
     * @throws IllegalArgumentException 当传入的消息类型不被支持时抛出异常
     */
    public static AbstractMessageProcessor<CompleteMessage> getProcessor(MessageType messageType) {
        return switch (messageType) {
            case LOGIN_MESSAGE -> LOGIN_PROCESSOR;
            case HEARTBEAT_MESSAGE -> HEART_BEAT_PROCESSOR;
            case PRIVATE_CHAT_MESSAGE -> PRIVATE_CHAT_PROCESSOR;
            default -> throw new IllegalArgumentException("unsupported message type");
        };
    }
}
