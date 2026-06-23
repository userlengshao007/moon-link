package com.moon.link.handler;

import com.moon.link.common.constant.ChannelAttrKey;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * 服务端空闲连接处理器。
 * <p>
 * IdleStateHandler 只负责检测空闲并触发事件；这个 Handler 负责处理事件。
 * 当服务端长时间没有收到客户端消息时，说明客户端可能断网、进程挂起或连接假死，
 * 此时主动关闭 channel，让 channelInactive 统一清理本地连接映射和 Redis 在线状态。
 */
@Slf4j
public class ServerIdleStateHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent idleStateEvent)) {
            super.userEventTriggered(ctx, evt);
            return;
        }

        if (idleStateEvent.state() != IdleState.READER_IDLE) {
            super.userEventTriggered(ctx, evt);
            return;
        }

        AttributeKey<Long> userIdKey = AttributeKey.valueOf(ChannelAttrKey.USER_ID);
        Long userId = ctx.channel().attr(userIdKey).get();

        // 这里只关闭连接，不直接删除 Map 或 Redis。
        // 关闭后 Netty 会触发 LinkChannelHandler.channelInactive，清理逻辑集中在那里，避免多处重复清理。
        log.warn("client read idle, close channel, userId: {}, remoteAddress: {}",
                userId, ctx.channel().remoteAddress());
        ctx.close();
    }
}
