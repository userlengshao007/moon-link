package com.moon.link.netty;

import com.moon.link.codec.MessageProtocolDecoder;
import com.moon.link.codec.MessageProtocolEncoder;
import com.moon.link.config.NettyConfig;
import com.moon.link.handler.LinkChannelHandler;
import com.moon.link.handler.ServerIdleStateHandler;
import com.moon.link.link.LinkConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class NettyServer {
    private final int port;
    public NettyServer(int port){
        this.port = port;
    }

    public void start() {
        NettyTransport transport = createTransport();
        EventLoopGroup bossGroup = transport.bossGroup;
        EventLoopGroup workerGroup = transport.workerGroup;
        // 生成对应的 机器 ID
        // 从系统属性获取 moon.link.machine.id，默认为0 如果大于0 就用配置的，如果为0 就去redis里边生成
        int configuredMachineId = Integer.getInteger("moon.link.machine.id", 0);

//        if (configuredMachineId > 0) {
//            LinkConfig.MACHINE_ID = configuredMachineId;
//        } else {
//            LinkConfig.MACHINE_ID = RedisClient.generateMachineId();
//        }

        log.info("moon-link machineId: {}", LinkConfig.MACHINE_ID);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workerGroup)
                    .channel(transport.serverSocketChannelClass)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    // 读空闲检测：超过配置时间没有收到客户端任何消息，就触发 READER_IDLE 事件。
                                    // 客户端正常会定时发心跳，所以只有断网、假死、长时间不发消息时才会触发。
                                    .addLast(new IdleStateHandler(
                                            NettyConfig.READER_IDLE_SECONDS,
                                            0,
                                            0,
                                            TimeUnit.SECONDS
                                    ))
                                    .addLast(new MessageProtocolDecoder())
                                    .addLast(new MessageProtocolEncoder())
                                    // 处理 IdleStateHandler 触发的空闲事件，只负责关闭 channel。
                                    // 真正的 UserChannelCtxMap 和 Redis 清理由 LinkChannelHandler.channelInactive 统一完成。
                                    .addLast(new ServerIdleStateHandler())
                                    .addLast(new LinkChannelHandler());
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("NettyServer started on port {}, transport: {}", port, transport.name);

            // 当前线程在这里等着，直到 Netty 服务端关闭
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("NettyServer interrupted", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private NettyTransport createTransport() {
        // Epoll 是 Linux 专属的高性能 IO 模型。这里先判断配置开关，再判断当前环境是否真的可用。
        if (NettyConfig.EPOLL_ENABLED && Epoll.isAvailable()) {
            log.info("Netty transport use Epoll");
            return new NettyTransport(
                    "epoll",
                    new EpollEventLoopGroup(1),
                    new EpollEventLoopGroup(),
                    EpollServerSocketChannel.class
            );
        }

        if (NettyConfig.EPOLL_ENABLED) {
            Throwable cause = Epoll.unavailabilityCause();
            log.info("Netty Epoll unavailable, fallback to NIO, cause: {}",
                    cause == null ? "unknown" : cause.toString());
        } else {
            log.info("Netty Epoll disabled by config, use NIO");
        }

        return new NettyTransport(
                "nio",
                new NioEventLoopGroup(1),
                new NioEventLoopGroup(),
                NioServerSocketChannel.class
        );
    }

    private static class NettyTransport {
        private final String name;
        private final EventLoopGroup bossGroup;
        private final EventLoopGroup workerGroup;
        private final Class<? extends ServerSocketChannel> serverSocketChannelClass;

        private NettyTransport(String name,
                               EventLoopGroup bossGroup,
                               EventLoopGroup workerGroup,
                               Class<? extends ServerSocketChannel> serverSocketChannelClass) {
            this.name = name;
            this.bossGroup = bossGroup;
            this.workerGroup = workerGroup;
            this.serverSocketChannelClass = serverSocketChannelClass;
        }
    }

}
