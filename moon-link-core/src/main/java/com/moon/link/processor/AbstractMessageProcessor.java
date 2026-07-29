package com.moon.link.processor;

import io.netty.channel.ChannelHandlerContext;

/**
 * 抽象类 消息处理
 *
 * @param <T> msg 参数
 */
public abstract class AbstractMessageProcessor<T> {
    /**
     * 处理一条已完成协议解码的消息。
     *
     * @param ctx 当前连接上下文
     * @param msg 待处理消息
     */
    public abstract void process(ChannelHandlerContext ctx, T msg);
}
