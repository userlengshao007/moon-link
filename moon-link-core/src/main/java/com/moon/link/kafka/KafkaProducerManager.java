package com.moon.link.kafka;

import com.moon.link.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka生产者管理器，负责创建和管理KafkaProducer实例的生命周期。
 * 采用双重检查锁定（DCL）模式实现线程安全的单例延迟初始化，
 * 确保在整个应用生命周期中只有一个KafkaProducer实例。
 */
@Slf4j
public class KafkaProducerManager {
    /**
     * Kafka生产者实例，使用volatile保证多线程环境下的可见性。
     * Key类型: String（通常为消息的key或用户ID）
     * Value类型: byte[]（序列化的消息内容）
     */
    private static volatile KafkaProducer<String, byte[]> producer;

    /**
     * 获取Kafka生产者实例，采用双重检查锁定模式实现线程安全的懒加载。
     * 首次调用时创建并初始化KafkaProducer，后续调用直接返回已有实例。
     *
     * @return KafkaProducer实例，用于发送消息到Kafka集群
     */
    public static KafkaProducer<String, byte[]> getProducer() {
        if (producer == null) {
            synchronized (KafkaProducerManager.class) {
                if (producer == null) {
                    producer = new KafkaProducer<>(buildProperties());
                    log.info("KafkaProducer started, bootstrapServers: {}", KafkaConfig.BOOTSTRAP_SERVERS);
                }
            }
        }
        return producer;
    }

    /**
     * 关闭Kafka生产者实例，释放相关资源。
     * 应在应用 shutdown hook 或服务停止时调用，确保消息发送完成并优雅关闭。
     */
    public static void shutdown() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * 构建Kafka生产者的配置属性。
     * 配置包括：Kafka集群地址、序列化器、确认机制和重试策略。
     *
     * @return Properties对象，包含Kafka生产者的所有配置项
     */
    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        return properties;
    }
}
