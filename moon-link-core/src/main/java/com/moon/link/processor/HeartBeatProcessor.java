package com.moon.link.processor;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import io.netty.channel.ChannelHandlerContext;

import static com.moon.link.common.enums.MessageType.HEARTBEAT_MESSAGE;

public class HeartBeatProcessor extends AbstractMessageProcessor<CompleteMessage> {
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        long uid = msg.getPacketHeader().getUid();

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
