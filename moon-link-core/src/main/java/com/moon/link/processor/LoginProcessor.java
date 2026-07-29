package com.moon.link.processor;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.config.KafkaConfig;
import com.moon.link.kafka.KafkaProducerManager;
import com.moon.link.link.LinkConfig;
import com.moon.link.redis.RedisClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

import static com.moon.link.common.enums.MessageType.LOGIN_MESSAGE;

/**
 * 登录消息处理器，建立用户在线路由并发布上线事件。
 */
@Slf4j
public class LoginProcessor extends AbstractMessageProcessor<CompleteMessage> {
    /** {@inheritDoc} */
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        long uid = msg.getPacketHeader().getUid();
        AttributeKey<Long> userIdKey = AttributeKey.valueOf(ChannelAttrKey.USER_ID);
        Long boundUserId = ctx.channel().attr(userIdKey).get();

        // 一个连接不允许在登录后切换用户，避免遗留旧用户的 Channel 映射。
        if (boundUserId != null && boundUserId != uid) {
            log.warn("channel repeat login with different user, boundUserId: {}, requestedUserId: {}, remoteAddress: {}",
                    boundUserId, uid, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 先标记新连接，再原子替换本地映射。旧连接随后断开时，条件删除不会误删新连接。
        ctx.channel().attr(userIdKey).set(uid);
        ChannelHandlerContext oldContext = UserChannelCtxMap.replace(uid, ctx);
        if (oldContext != null && oldContext != ctx) {
            log.info("replace old user connection, userId: {}, oldRemoteAddress: {}, newRemoteAddress: {}",
                    uid, oldContext.channel().remoteAddress(), ctx.channel().remoteAddress());
            oldContext.close();
        }

        CompleteMessage response = CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(uid)
                        .setMessageType(LOGIN_MESSAGE.getType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setContent("login success")
                        .setTimeStamp(System.currentTimeMillis())
                        .build())
                .build();
        ctx.writeAndFlush(response);
        // 写入跨节点路由后发布上线事件，触发 IM 服务进行离线消息补偿。
        RedisClient.setUserOnline(uid, LinkConfig.MACHINE_ID);
        sendUserOnlineEvent(uid);
    }

    /**
     * 异步发布用户上线事件，生产失败仅记录告警，不中断已完成的登录流程。
     *
     * @param uid 用户 ID
     */
    private void sendUserOnlineEvent(long uid) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                KafkaConfig.USER_ONLINE_TOPIC,
                String.valueOf(uid),
                String.valueOf(uid).getBytes(StandardCharsets.UTF_8)
        );

        KafkaProducerManager.getProducer().send(record, (metadata, exception) -> {
            if (exception != null) {
                log.warn("send user online event failed, uid: {}", uid, exception);
                return;
            }
            log.info("send user online event success, uid: {}, topic: {}, partition: {}, offset: {}",
                    uid, metadata.topic(), metadata.partition(), metadata.offset());
        });
    }
}
