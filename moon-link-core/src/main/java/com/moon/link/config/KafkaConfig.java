package com.moon.link.config;

/**
 * Kafka 集群和业务主题配置。
 */
public final class KafkaConfig {
    /** Kafka broker 地址列表。 */
    public static final String BOOTSTRAP_SERVERS =
            System.getProperty("moon.link.kafka.bootstrap-servers", "127.0.0.1:9092");

    /** 私聊消息主题。 */
    public static final String PRIVATE_CHAT_TOPIC =
            System.getProperty("moon.link.kafka.private-chat-topic", "private_chat");

    /** 群聊消息主题。 */
    public static final String GROUP_CHAT_TOPIC =
            System.getProperty("moon.link.kafka.group-chat-topic", "group_chat");

    /** 用户上线事件主题。 */
    public static final String USER_ONLINE_TOPIC =
            System.getProperty("moon.link.kafka.user-online-topic", "user_online");

    private KafkaConfig() {
    }
}
