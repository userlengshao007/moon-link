package com.moon.link.processor;

import io.netty.channel.ChannelHandlerContext;

/**
 * 抽象类 消息处理
 *
 * @param <T> msg 参数
 */
public abstract class AbstractMessageProcessor<T> {
    public abstract void process(ChannelHandlerContext ctx,T msg);
}
