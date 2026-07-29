package com.moon.im.consumer;

import com.moon.im.constant.MessagePushStatus;
import com.moon.im.domain.SingleChatMessage;
import com.moon.im.service.SingleChatMessageService;
import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 用户上线事件消费者，负责补推该用户离线期间未成功送达的私聊消息。
 */
@Slf4j
@Component
public class UserOnlineConsumer {

    private static final int COMPENSATE_LIMIT = 100;

    private final SingleChatMessageService singleChatMessageService;
    private final PushServiceGrpc.PushServiceBlockingStub pushServiceBlockingStub;

    /**
     * 创建用户上线事件消费者。
     *
     * @param singleChatMessageService 单聊消息服务
     * @param pushServiceBlockingStub 长连接推送服务客户端
     */
    public UserOnlineConsumer(SingleChatMessageService singleChatMessageService,
                              PushServiceGrpc.PushServiceBlockingStub pushServiceBlockingStub) {
        this.singleChatMessageService = singleChatMessageService;
        this.pushServiceBlockingStub = pushServiceBlockingStub;
    }

    /**
     * 消费用户上线事件，并按时间顺序补推一批失败消息。
     *
     * @param record 用户上线 Kafka 消息
     */
    @KafkaListener(
            topics = "${moon-im.kafka.user-online-topic:user_online}",
            groupId = "${moon-im.kafka.user-online-group-id:moon-im-user-online}"
    )
    public void consumeUserOnline(ConsumerRecord<String, byte[]> record) {
        Long userId = parseUserId(record);
        if (userId == null) {
            return;
        }

        // 每次只处理有限数量，防止单个用户积压过多时长时间占用消费线程。
        List<SingleChatMessage> messages =
                singleChatMessageService.listUnpushedMessages(userId, COMPENSATE_LIMIT);
        if (messages.isEmpty()) {
            log.info("no offline private chat to compensate, userId: {}", userId);
            return;
        }

        log.info("start compensate offline private chat, userId: {}, count: {}",
                userId, messages.size());

        for (SingleChatMessage message : messages) {
            compensateOne(message);
        }
    }

    /**
     * 从上线事件载荷中解析用户 ID。
     *
     * @param record Kafka 消息
     * @return 用户 ID；消息非法时返回 {@code null}
     */
    private Long parseUserId(ConsumerRecord<String, byte[]> record) {
        try {
            String payload = new String(record.value(), StandardCharsets.UTF_8);
            return Long.parseLong(payload);
        } catch (Exception e) {
            log.error("parse user online event failed, topic: {}, partition: {}, offset: {}, key: {}",
                    record.topic(), record.partition(), record.offset(), record.key(), e);
            return null;
        }
    }

    /**
     * 调用长连接服务补推一条消息，并在成功后更新持久化状态。
     *
     * @param message 待补推消息
     */
    private void compensateOne(SingleChatMessage message) {
        PushGrpc.Push2UserRequest request = PushGrpc.Push2UserRequest.newBuilder()
                .setToId(message.getToUserId())
                .setMessage(PushGrpc.PushMessageBody.newBuilder()
                        .setFromUserId(message.getFromUserId())
                        .setTimeStamp(System.currentTimeMillis())
                        .setMessageType(message.getMessageType())
                        .setContent(message.getContent())
                        .build())
                .build();

        try {
            PushGrpc.Push2UserResponse response = pushServiceBlockingStub.push2User(request);
            if (response.getSuccess()) {
                singleChatMessageService.updatePushStatus(message.getId(), MessagePushStatus.PUSH_SUCCESS);
                log.info("compensate offline private chat success, id: {}, toUserId: {}, seqId: {}",
                        message.getId(), message.getToUserId(), message.getSeqId());
                return;
            }

            log.info("compensate offline private chat not success, id: {}, toUserId: {}, code: {}, msg: {}",
                    message.getId(), message.getToUserId(), response.getCode(), response.getMsg());
        } catch (Exception e) {
            log.error("compensate offline private chat failed, id: {}, toUserId: {}",
                    message.getId(), message.getToUserId(), e);
        }
    }
}
