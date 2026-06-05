package com.moon.link.handler;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.enums.MessageType;
import com.moon.link.processor.AbstractMessageProcessor;
import com.moon.link.processor.ProcessorFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

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
}
