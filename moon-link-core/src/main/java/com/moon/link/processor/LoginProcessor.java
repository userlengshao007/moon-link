package com.moon.link.processor;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.enums.MessageType;
import com.moon.link.link.LinkConfig;
import com.moon.link.redis.RedisClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

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
        // 给 ctx 加上对应的 USER_ID
        AttributeKey<Long> userIdKey = AttributeKey.valueOf(ChannelAttrKey.USER_ID);
        ctx.channel().attr(userIdKey).set(uid);
        ctx.writeAndFlush(response);
        // Redis 写入 uid -> machineId 过期时间300秒
        RedisClient.setUserOnline(uid, LinkConfig.MACHINE_ID);
    }
}
