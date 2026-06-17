package com.moon.link.config;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS =
            System.getProperty("moon.link.kafka.bootstrap-servers", "127.0.0.1:9092");

    public static final String PRIVATE_CHAT_TOPIC =
            System.getProperty("moon.link.kafka.private-chat-topic", "private_chat");

    public static final String GROUP_CHAT_TOPIC =
            System.getProperty("moon.link.kafka.group-chat-topic", "group_chat");

    public static final String USER_ONLINE_TOPIC =
            System.getProperty("moon.link.kafka.user-online-topic", "user_online");
}
