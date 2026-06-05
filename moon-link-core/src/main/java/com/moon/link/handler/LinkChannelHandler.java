package com.moon.link.handler;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.constant.ChannelAttrKey;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.enums.MessageType;
import com.moon.link.processor.AbstractMessageProcessor;
import com.moon.link.processor.ProcessorFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;

public class LinkChannelHandler extends SimpleChannelInboundHandler<CompleteMessage> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CompleteMessage msg) throws Exception {
        int type = msg.getPacketHeader().getMessageType();
        MessageType messageType = MessageType.fromType(type);

        if (messageType == null) {
            ctx.close();
            return;
        }

        AbstractMessageProcessor<CompleteMessage> processor = ProcessorFactory.getProcessor(messageType);
        processor.process(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 当链接断开的时候，删除对应 map 中的元素
        AttributeKey<Long> userIdKey = AttributeKey.valueOf(ChannelAttrKey.USER_ID);
        Long userId = ctx.channel().attr(userIdKey).get();
        if(userId != null){
            UserChannelCtxMap.remove(userId);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
