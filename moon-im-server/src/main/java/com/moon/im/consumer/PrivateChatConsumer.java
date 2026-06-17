package com.moon.im.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.moon.im.constant.MessagePushStatus;
import com.moon.im.domain.SingleChatMessage;
import com.moon.im.mapper.SingleChatMessageMapper;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.enums.MessageType;
import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 私聊消息Kafka消费者，负责从Kafka队列中消费私聊消息并通过gRPC推送到目标用户。
 * 作为moon-link系统的消息处理链路中的一环，接收来自PrivateChatProcessor发送到Kafka的消息，
 * 然后调用PushService将消息推送给在线用户。
 */
@Slf4j
@Component
public class PrivateChatConsumer {

    private static final String SINGLE_CHAT_SEQ_KEY_PREFIX = "moon:im:single:seq:";

    /**
     * gRPC推送服务阻塞式存根，用于调用moon-link的推送接口。
     * 通过构造函数注入，由Spring容器管理生命周期。
     */
    private final PushServiceGrpc.PushServiceBlockingStub pushServiceBlockingStub;
    private final SingleChatMessageMapper singleChatMessageMapper;
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 构造函数，注入PushService的gRPC存根。
     *
     * @param pushServiceBlockingStub PushService的阻塞式gRPC存根
     */
    public PrivateChatConsumer(PushServiceGrpc.PushServiceBlockingStub pushServiceBlockingStub,
                               SingleChatMessageMapper singleChatMessageMapper,
                               StringRedisTemplate stringRedisTemplate) {
        this.pushServiceBlockingStub = pushServiceBlockingStub;
        this.singleChatMessageMapper = singleChatMessageMapper;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * Kafka消息监听方法，消费私聊消息主题中的消息并执行推送逻辑。
     * 主要流程：解析Protobuf消息 -> 提取消息内容 -> 构建gRPC请求 -> 调用推送服务 -> 记录推送结果。
     *
     * @param record Kafka消费者记录，包含消息的key、value（序列化的CompleteMessage）、topic、partition等信息
     */
    @KafkaListener(
            topics = "${moon-im.kafka.private-chat-topic:private_chat}",
            groupId = "${spring.kafka.consumer.group-id:moon-im-server}"
    )
    public void consumePrivateChat(ConsumerRecord<String, byte[]> record) {
        CompleteMessage message;
        try {
            message = CompleteMessage.parseFrom(record.value());
        } catch (InvalidProtocolBufferException e) {
            log.error("parse private chat kafka message failed, topic: {}, partition: {}, offset: {}",
                    record.topic(), record.partition(), record.offset(), e);
            return;
        }

        long fromUserId = message.getPacketBody().getFromUserId();
        long toId = message.getPacketBody().getToId();
        String content = message.getPacketBody().getContent();
        int messageType = message.getPacketBody().getMessageType();

        log.info("consume private chat, key: {}, from: {}, to: {}, content: {}",
                record.key(), fromUserId, toId, content);

        SingleChatMessage chatMessage = savePrivateChatMessage(record, fromUserId, toId, content, messageType);

        // 构建gRPC推送请求并调用推送服务
        PushGrpc.Push2UserRequest request = buildPushRequest(fromUserId, toId, content);

        try {
            PushGrpc.Push2UserResponse response = pushServiceBlockingStub.push2User(request);
            updatePushStatus(chatMessage, response.getSuccess()
                    ? MessagePushStatus.PUSH_SUCCESS
                    : MessagePushStatus.PUSH_FAILED);
            log.info("push private chat result, toId: {}, success: {}, code: {}, msg: {}",
                    toId, response.getSuccess(), response.getCode(), response.getMsg());
        } catch (Exception e) {
            updatePushStatus(chatMessage, MessagePushStatus.PUSH_FAILED);
            log.error("push private chat failed, from: {}, to: {}", fromUserId, toId, e);
        }
    }

    private SingleChatMessage savePrivateChatMessage(ConsumerRecord<String, byte[]> record,
                                                     long fromUserId,
                                                     long toId,
                                                     String content,
                                                     int messageType) {
        String cid = buildCid(fromUserId, toId);
        Long seqId = nextSeqId(cid);
        LocalDateTime now = LocalDateTime.now();

        SingleChatMessage chatMessage = new SingleChatMessage();
        chatMessage.setCid(cid);
        chatMessage.setSeqId(seqId);
        chatMessage.setFromUserId(fromUserId);
        chatMessage.setToUserId(toId);
        chatMessage.setContent(content);
        chatMessage.setMessageType(messageType);
        chatMessage.setStatus(MessagePushStatus.SAVED);
        chatMessage.setKafkaTopic(record.topic());
        chatMessage.setKafkaPartition(record.partition());
        chatMessage.setKafkaOffset(record.offset());
        chatMessage.setCreateTime(now);
        chatMessage.setUpdateTime(now);

        singleChatMessageMapper.insert(chatMessage);
        log.info("save private chat message, id: {}, cid: {}, seqId: {}",
                chatMessage.getId(), cid, seqId);

        return chatMessage;
    }

    private void updatePushStatus(SingleChatMessage chatMessage, int status) {
        chatMessage.setStatus(status);
        chatMessage.setUpdateTime(LocalDateTime.now());
        singleChatMessageMapper.updateById(chatMessage);
    }

    private String buildCid(long fromUserId, long toId) {
        long minUserId = Math.min(fromUserId, toId);
        long maxUserId = Math.max(fromUserId, toId);
        return minUserId + "_" + maxUserId;
    }

    private Long nextSeqId(String cid) {
        String key = SINGLE_CHAT_SEQ_KEY_PREFIX + cid;
        return stringRedisTemplate.opsForValue().increment(key);
    }

    /**
     * 构建gRPC推送请求对象，将私聊消息封装为Push2UserRequest格式。
     *
     * @param fromUserId 发送者用户ID
     * @param toId 接收者用户ID
     * @param content 消息内容
     * @return 配置完成的Push2UserRequest对象，包含消息体和接收者信息
     */
    private PushGrpc.Push2UserRequest buildPushRequest(long fromUserId, long toId, String content) {
        PushGrpc.PushMessageBody messageBody = PushGrpc.PushMessageBody.newBuilder()
                .setFromUserId(fromUserId)
                .setTimeStamp(System.currentTimeMillis())
                .setMessageType(MessageType.PRIVATE_CHAT_MESSAGE.getType())
                .setContent(content)
                .build();

        return PushGrpc.Push2UserRequest.newBuilder()
                .setToId(toId)
                .setMessage(messageBody)
                .build();
    }
}
