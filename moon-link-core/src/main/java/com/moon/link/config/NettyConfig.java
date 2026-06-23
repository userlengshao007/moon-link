package com.moon.link.config;

public class NettyConfig {
    public static final int NETTY_PORT = Integer.getInteger("moon.link.netty.port", 9999);

    /**
     * 是否允许服务端优先使用 Epoll。
     * <p>
     * Epoll 只能在 Linux 环境下使用；macOS / Windows 本地开发时即使这里为 true，
     * NettyServer 也会自动降级成 NIO。
     */
    public static final boolean EPOLL_ENABLED =
            Boolean.parseBoolean(System.getProperty("moon.link.netty.epoll.enabled", "true"));

    /**
     * 服务端读空闲超时时间。
     * <p>
     * 客户端正常情况下应该定时发送心跳消息；如果服务端超过这个时间没有读到任何客户端数据，
     * 就认为连接可能已经假死，后续由 IdleStateHandler 触发关闭。
     */
    public static final int READER_IDLE_SECONDS =
            Integer.getInteger("moon.link.netty.reader-idle-seconds", 90);
}
