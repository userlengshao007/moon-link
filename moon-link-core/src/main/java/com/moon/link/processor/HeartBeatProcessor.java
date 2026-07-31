package com.moon.link.processor;

import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.heartbeat.OnlineRenewManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

import static com.moon.link.common.enums.MessageType.HEARTBEAT_MESSAGE;

/**
 * 心跳处理器，记录本地活跃状态并响应 pong。
 */
public class HeartBeatProcessor extends AbstractMessageProcessor<CompleteMessage> {
    /** {@inheritDoc} */
    @Override
    public void process(ChannelHandlerContext ctx, CompleteMessage msg) {
        AttributeKey<Long> userIdKey = AttributeKey.valueOf(ChannelAttrKey.USER_ID);
        Long userId = ctx.channel().attr(userIdKey).get();
        if (userId == null) {
            ctx.close();
            return;
        }

        // 不信任心跳报文中的 uid，以登录成功后绑定到 Channel 的用户身份为准。
        OnlineRenewManager.markActive(userId);

        CompleteMessage response = CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(userId)
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
