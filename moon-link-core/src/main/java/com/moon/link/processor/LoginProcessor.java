package com.moon.link.processor;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.enums.MessageType;
import io.netty.channel.ChannelHandlerContext;

import static com.moon.link.common.enums.MessageType.LOGIN_MESSAGE;

public class LoginProcessor extends AbstractMessageProcessor<CompleteMessage> {
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        // 获取用户id
        long uid = msg.getPacketHeader().getUid();

        UserChannelCtxMap.add(uid,ctx);

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
    }
}
