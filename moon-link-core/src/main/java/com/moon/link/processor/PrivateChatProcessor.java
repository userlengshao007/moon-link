package com.moon.link.processor;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.enums.MessageType;
import com.moon.link.config.KafkaConfig;
import com.moon.link.kafka.KafkaProducerManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 私聊消息处理器，负责处理点对点聊天消息。
 * 接收客户端发送的私聊消息，将其发送到Kafka消息队列进行异步处理和分发，
 * 并向客户端发送ACK确认响应。
 */
@Slf4j
public class PrivateChatProcessor extends AbstractMessageProcessor<CompleteMessage> {
    /**
     * 处理私聊消息的核心方法。
     * 将消息封装为Kafka ProducerRecord并异步发送到Kafka集群，
     * 根据发送结果向客户端返回成功或失败的ACK响应。
     *
     * @param ctx Netty通道上下文，用于向客户端发送响应
     * @param msg 完整的消息对象，包含消息头、消息体和发送者/接收者信息
     */
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        long fromUserId = msg.getPacketBody().getFromUserId();
        long toId = msg.getPacketBody().getToId();

        // 构建Kafka消息记录，使用有序的Kafka Key保证同一会话的消息路由到相同分区
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                KafkaConfig.PRIVATE_CHAT_TOPIC,
                buildKafkaKey(fromUserId, toId),
                msg.toByteArray()
        );

        // 异步发送消息到Kafka，并根据发送结果返回ACK响应
        KafkaProducerManager.getProducer().send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("private chat message sent to kafka, topic: {}, partition: {}, offset: {}, from: {}, to: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), fromUserId, toId);
                writeAck(ctx, msg, true, "private chat accepted");
                return;
            }

            log.error("private chat message send to kafka failed, from: {}, to: {}",
                    fromUserId, toId, exception);
            writeAck(ctx, msg, false, "private chat send failed");
        });
    }

    /**
     * 构建Kafka消息的Key，确保同一对话的消息能够路由到相同的Kafka分区，
     * 保证消息的顺序性。通过将较小的ID放在前面，确保双向消息的一致性。
     *
     * @param fromUserId 发送者用户ID
     * @param toId 接收者用户ID
     * @return 格式化的Kafka Key，格式为"smallId_largeId"
     */
    private String buildKafkaKey(long fromUserId, long toId) {
        long small = Math.min(fromUserId, toId);
        long large = Math.max(fromUserId, toId);
        return small + "_" + large;
    }

    /**
     * 向客户端写入ACK确认响应消息，告知客户端消息是否被成功处理。
     * 仅在通道处于活跃状态时才发送响应，避免在连接关闭后尝试写入。
     *
     * @param ctx Netty通道上下文
     * @param source 原始消息对象，用于提取消息头和构造响应
     * @param success 消息处理是否成功
     * @param content ACK响应的内容描述
     */
    private void writeAck(ChannelHandlerContext ctx, CompleteMessage source, boolean success, String content) {
        CompleteMessage ack = CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(source.getPacketHeader().getUid())
                        .setMessageType(MessageType.ACK_MESSAGE.getType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(source.getPacketBody().getFromUserId())
                        .setToId(source.getPacketBody().getToId())
                        .setMessageType(MessageType.ACK_MESSAGE.getType())
                        .setTimeStamp(System.currentTimeMillis())
                        .setContent(success ? content : "ERROR:" + content)
                        .build())
                .build();

        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(ack);
        }
    }
}
