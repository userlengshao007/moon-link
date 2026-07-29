package com.moon.link.processor;

import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.redis.RedisClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

import static com.moon.link.common.enums.MessageType.HEARTBEAT_MESSAGE;

/**
 * 心跳处理器，响应 pong 并按固定频率刷新用户在线状态。
 */
public class HeartBeatProcessor extends AbstractMessageProcessor<CompleteMessage> {
    /** {@inheritDoc} */
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        long uid = msg.getPacketHeader().getUid();

        AttributeKey<Long> heartBeatTimesKey = AttributeKey.valueOf(ChannelAttrKey.HEARTBEAT_TIMES);
        Long lastTimes = ctx.channel().attr(heartBeatTimesKey).get();
        long heartBeatTimes = lastTimes == null ? 1 : lastTimes + 1;
        ctx.channel().attr(heartBeatTimesKey).set(heartBeatTimes);

        // 不必每次心跳都访问 Redis；每三次续期可降低高连接数下的写压力。
        if (heartBeatTimes % 3 == 0) {
            RedisClient.expireUserOnline(uid);
        }

        CompleteMessage response = CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(uid)
                        .setMessageType(HEARTBEAT_MESSAGE.getType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setContent("pong")
                        .setTimeStamp(System.currentTimeMillis())
                        .build())
                .build();

        ctx.writeAndFlush(response);
    }
}
