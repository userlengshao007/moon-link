package com.moon.link.processor;

import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.redis.RedisClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

import static com.moon.link.common.enums.MessageType.HEARTBEAT_MESSAGE;

public class HeartBeatProcessor extends AbstractMessageProcessor<CompleteMessage> {
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        long uid = msg.getPacketHeader().getUid();

        AttributeKey<Long> heartBeatTimesKey = AttributeKey.valueOf(ChannelAttrKey.HEARTBEAT_TIMES);
        Long lastTimes = ctx.channel().attr(heartBeatTimesKey).get();
        long heartBeatTimes = lastTimes == null ? 1 : lastTimes + 1;
        ctx.channel().attr(heartBeatTimesKey).set(heartBeatTimes);

        // 每三次心跳做一个续期
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
