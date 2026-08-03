package com.moon.link.client;

import com.moon.link.codec.MessageProtocolDecoder;
import com.moon.link.codec.MessageProtocolEncoder;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.enums.MessageType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Netty 长连接压测客户端。
 * <p>
 * 在 Windows 电脑上运行，通过一个共享的 {@link NioEventLoopGroup} 管理大量连接，
 * 分批连接 Mac 上的 moon-link 网关。每条连接使用不同用户 ID 登录，并定时发送心跳。
 * 所有压测参数都集中定义为 Java 常量，不依赖 VM Options。
 */
@Slf4j
public final class ConnectionLoadTestClient {
    /** Mac 服务端当前局域网 IP。Mac IP 变化时只需要修改这里。 */
    private static final String SERVER_HOST = "192.168.76.214";
    /** Netty 服务监听端口。 */
    private static final int SERVER_PORT = 9999;
    /** 本轮压测尝试建立的连接总数。 */
    private static final int TARGET_CONNECTIONS = 6000;
    /** 每批发起的连接数量。 */
    private static final int CONNECTION_BATCH_SIZE = 20;
    /** 两批建连任务之间的间隔；20 条/100 毫秒约等于每秒 200 条。 */
    private static final int CONNECTION_BATCH_INTERVAL_MILLIS = 100;
    /** 单条 TCP 连接的建立超时时间。 */
    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    /** 客户端心跳发送间隔。 */
    private static final int HEARTBEAT_INTERVAL_SECONDS = 30;
    /** 完成全部建连尝试后继续保持连接的时长。 */
    private static final int HOLD_DURATION_SECONDS = 60 * 60;
    /** 聚合统计日志输出间隔。 */
    private static final int STATISTICS_INTERVAL_SECONDS = 10;
    /** 压测用户 ID 起始值，避免和普通测试账号冲突。 */
    private static final long START_USER_ID = 1_000_000L;
    /** 测试登录凭证。当前服务端尚未执行真实 Token 校验。 */
    private static final String TEST_TOKEN = "load-test-token";
    /** 每个压测 Channel 保存用户 ID 的属性键。 */
    private static final AttributeKey<Long> CLIENT_USER_ID_KEY =
            AttributeKey.valueOf("loadTestClientUserId");

    /** 所有已建立 Channel 的集合，用于测试结束时批量关闭。 */
    private static final ChannelGroup CHANNELS =
            new DefaultChannelGroup("load-test-channels", GlobalEventExecutor.INSTANCE);
    /** 已发起的连接数量。 */
    private static final AtomicInteger ATTEMPTED_CONNECTIONS = new AtomicInteger();
    /** TCP 建立成功总数。 */
    private static final AtomicInteger CONNECTED_CONNECTIONS = new AtomicInteger();
    /** 登录成功总数。 */
    private static final AtomicInteger LOGIN_SUCCESSES = new AtomicInteger();
    /** 当前仍然活跃的连接数。 */
    private static final AtomicInteger ACTIVE_CONNECTIONS = new AtomicInteger();
    /** TCP 建立失败总数。 */
    private static final AtomicInteger CONNECT_FAILURES = new AtomicInteger();
    /** 建立成功后异常断开的连接总数。 */
    private static final AtomicInteger DISCONNECTED_CONNECTIONS = new AtomicInteger();
    /** Channel 处理异常总数。 */
    private static final LongAdder CHANNEL_EXCEPTIONS = new LongAdder();
    /** 已发送心跳总数。 */
    private static final LongAdder HEARTBEATS_SENT = new LongAdder();
    /** 已收到 pong 总数。 */
    private static final LongAdder PONGS_RECEIVED = new LongAdder();
    /** 所有已收到 pong 的累计延迟，单位为纳秒。 */
    private static final LongAdder PONG_LATENCY_NANOS = new LongAdder();
    /** 已观察到的最大 pong 延迟，单位为纳秒。 */
    private static final AtomicLong MAX_PONG_LATENCY_NANOS = new AtomicLong();
    /** 是否已经启动连接保持倒计时，防止重复创建结束任务。 */
    private static final AtomicBoolean HOLD_TIMER_STARTED = new AtomicBoolean(false);
    /** 测试是否正在停止。 */
    private static final AtomicBoolean STOPPING = new AtomicBoolean(false);

    /**
     * 启动多连接压测。
     *
     * @param args 命令行参数，本客户端不读取该参数
     * @throws InterruptedException 当前线程等待测试结束时被中断
     */
    public static void main(String[] args) throws InterruptedException {
        validateConfiguration();

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        CountDownLatch testFinished = new CountDownLatch(1);
        ScheduledFuture<?> connectFuture = null;
        ScheduledFuture<?> statisticsFuture = null;

        try {
            Bootstrap bootstrap = buildBootstrap(eventLoopGroup);
            ConnectionBatchTask connectionTask = new ConnectionBatchTask(
                    bootstrap,
                    eventLoopGroup,
                    testFinished
            );

            connectFuture = eventLoopGroup.next().scheduleAtFixedRate(
                    connectionTask,
                    0,
                    CONNECTION_BATCH_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS
            );
            connectionTask.setScheduledFuture(connectFuture);

            statisticsFuture = eventLoopGroup.next().scheduleAtFixedRate(
                    () -> reportStatistics("running"),
                    STATISTICS_INTERVAL_SECONDS,
                    STATISTICS_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );

            log.info("load test started, server: {}:{}, targetConnections: {}, connectRatePerSecond: {}, "
                            + "heartbeatIntervalSeconds: {}, holdDurationSeconds: {}",
                    SERVER_HOST,
                    SERVER_PORT,
                    TARGET_CONNECTIONS,
                    calculateConnectRatePerSecond(),
                    HEARTBEAT_INTERVAL_SECONDS,
                    HOLD_DURATION_SECONDS);

            testFinished.await();
            STOPPING.set(true);
            reportStatistics("test-complete");
        } finally {
            if (connectFuture != null) {
                connectFuture.cancel(false);
            }
            if (statisticsFuture != null) {
                statisticsFuture.cancel(false);
            }
            CHANNELS.close().awaitUninterruptibly();
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
            log.info("load test resources released");
        }
    }

    /**
     * 创建所有连接共享的客户端启动配置。
     *
     * @param eventLoopGroup 客户端 I/O 事件循环组
     * @return Netty 客户端启动配置
     */
    private static Bootstrap buildBootstrap(EventLoopGroup eventLoopGroup) {
        return new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        Long userId = channel.attr(CLIENT_USER_ID_KEY).get();
                        if (userId == null) {
                            channel.close();
                            return;
                        }

                        channel.pipeline()
                                .addLast(new MessageProtocolDecoder())
                                .addLast(new MessageProtocolEncoder())
                                .addLast(new LoadClientHandler(userId));
                    }
                });
    }

    /**
     * 校验硬编码的压测参数，避免无效配置运行到一半才暴露。
     */
    private static void validateConfiguration() {
        if (TARGET_CONNECTIONS <= 0) {
            throw new IllegalArgumentException("target connections must be greater than zero");
        }
        if (CONNECTION_BATCH_SIZE <= 0 || CONNECTION_BATCH_INTERVAL_MILLIS <= 0) {
            throw new IllegalArgumentException("connection batch configuration must be greater than zero");
        }
        if (HEARTBEAT_INTERVAL_SECONDS <= 0 || HOLD_DURATION_SECONDS <= 0) {
            throw new IllegalArgumentException("heartbeat interval and hold duration must be greater than zero");
        }
    }

    /**
     * 根据批次大小和执行间隔计算理论每秒建连数量。
     *
     * @return 理论每秒建连数量
     */
    private static int calculateConnectRatePerSecond() {
        return CONNECTION_BATCH_SIZE * 1000 / CONNECTION_BATCH_INTERVAL_MILLIS;
    }

    /**
     * 构建登录或心跳消息。
     *
     * @param userId 当前连接对应的用户 ID
     * @param messageType 消息类型
     * @param content 消息内容
     * @return 完整协议消息
     */
    private static CompleteMessage buildMessage(long userId, MessageType messageType, String content) {
        return CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setAppId(1)
                        .setUid(userId)
                        .setToken(TEST_TOKEN)
                        .setCompression(0)
                        .setEncryption(0)
                        .setMessageType(messageType.getType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(userId)
                        .setTimeStamp(System.currentTimeMillis())
                        .setMessageType(messageType.getType())
                        .setContent(content)
                        .build())
                .build();
    }

    /**
     * 输出一次聚合压测指标，不为单条连接打印 info 日志。
     *
     * @param phase 当前测试阶段
     */
    private static void reportStatistics(String phase) {
        long pongCount = PONGS_RECEIVED.sum();
        long averageLatencyNanos = pongCount == 0 ? 0 : PONG_LATENCY_NANOS.sum() / pongCount;

        log.info("load test statistics, phase: {}, attempted: {}, connected: {}, loginSuccess: {}, active: {}, "
                        + "connectFailure: {}, disconnected: {}, channelException: {}, heartbeatSent: {}, "
                        + "pongReceived: {}, averagePongLatencyMs: {}, maxPongLatencyMs: {}",
                phase,
                ATTEMPTED_CONNECTIONS.get(),
                CONNECTED_CONNECTIONS.get(),
                LOGIN_SUCCESSES.get(),
                ACTIVE_CONNECTIONS.get(),
                CONNECT_FAILURES.get(),
                DISCONNECTED_CONNECTIONS.get(),
                CHANNEL_EXCEPTIONS.sum(),
                HEARTBEATS_SENT.sum(),
                pongCount,
                TimeUnit.NANOSECONDS.toMillis(averageLatencyNanos),
                TimeUnit.NANOSECONDS.toMillis(MAX_PONG_LATENCY_NANOS.get()));
    }

    /**
     * 原子更新最大心跳延迟。
     *
     * @param currentLatencyNanos 本次心跳延迟
     */
    private static void updateMaxPongLatency(long currentLatencyNanos) {
        MAX_PONG_LATENCY_NANOS.accumulateAndGet(currentLatencyNanos, Math::max);
    }

    /**
     * 按固定批次发起异步连接，避免瞬间创建大量连接冲击客户端和服务端。
     */
    private static final class ConnectionBatchTask implements Runnable {
        /** 客户端启动配置模板。 */
        private final Bootstrap bootstrap;
        /** 用于创建连接保持结束任务的事件循环组。 */
        private final EventLoopGroup eventLoopGroup;
        /** 测试结束信号。 */
        private final CountDownLatch testFinished;
        /** 当前定时建连任务，用于完成目标数量后取消。 */
        private ScheduledFuture<?> scheduledFuture;

        private ConnectionBatchTask(Bootstrap bootstrap,
                                    EventLoopGroup eventLoopGroup,
                                    CountDownLatch testFinished) {
            this.bootstrap = bootstrap;
            this.eventLoopGroup = eventLoopGroup;
            this.testFinished = testFinished;
        }

        /**
         * 保存当前定时建连任务。
         *
         * @param future 定时任务
         */
        private void setScheduledFuture(ScheduledFuture<?> future) {
            this.scheduledFuture = future;
        }

        /**
         * 发起一批异步TCP连接。
         */
        @Override
        public void run() {
            if (STOPPING.get()) {
                return;
            }

            for (int index = 0; index < CONNECTION_BATCH_SIZE; index++) {
                int connectionIndex = ATTEMPTED_CONNECTIONS.getAndIncrement();
                if (connectionIndex >= TARGET_CONNECTIONS) {
                    ATTEMPTED_CONNECTIONS.decrementAndGet();
                    startHoldTimer();
                    return;
                }

                long userId = START_USER_ID + connectionIndex;
                ChannelFuture connectFuture = bootstrap.clone()
                        .attr(CLIENT_USER_ID_KEY, userId)
                        .connect(SERVER_HOST, SERVER_PORT);
                connectFuture.addListener(future -> {
                    if (!future.isSuccess()) {
                        CONNECT_FAILURES.incrementAndGet();
                        log.debug("connect failed, userId: {}", userId, future.cause());
                    }
                });
            }

            if (ATTEMPTED_CONNECTIONS.get() >= TARGET_CONNECTIONS) {
                startHoldTimer();
            }
        }

        /**
         * 完成全部建连尝试后取消建连任务，并从此时开始计算连接保持时长。
         */
        private void startHoldTimer() {
            if (!HOLD_TIMER_STARTED.compareAndSet(false, true)) {
                return;
            }
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            log.info("all connection attempts submitted, attempted: {}, holdDurationSeconds: {}",
                    ATTEMPTED_CONNECTIONS.get(), HOLD_DURATION_SECONDS);
            eventLoopGroup.next().schedule(
                    testFinished::countDown,
                    HOLD_DURATION_SECONDS,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * 单条压测连接的登录、心跳和生命周期处理器。
     */
    private static final class LoadClientHandler extends SimpleChannelInboundHandler<CompleteMessage> {
        /** 当前连接对应的唯一用户 ID。 */
        private final long userId;
        /** 当前连接的定时心跳任务。 */
        private ScheduledFuture<?> heartbeatFuture;
        /** 最近一次心跳发送时间，使用单调时钟计算耗时。 */
        private long lastHeartbeatNanos;
        /** 当前连接是否已经完成登录。 */
        private boolean loggedIn;
        /** 当前连接是否已经计入活跃连接数。 */
        private boolean activeCounted;

        private LoadClientHandler(long userId) {
            this.userId = userId;
        }

        /**
         * TCP连接建立后立即发送登录消息。
         *
         * @param ctx Channel上下文
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (STOPPING.get()) {
                ctx.close();
                return;
            }

            CHANNELS.add(ctx.channel());
            CONNECTED_CONNECTIONS.incrementAndGet();
            ACTIVE_CONNECTIONS.incrementAndGet();
            activeCounted = true;
            ctx.writeAndFlush(buildMessage(userId, MessageType.LOGIN_MESSAGE, "login request"));
        }

        /**
         * 处理登录响应和心跳pong。
         *
         * @param ctx Channel上下文
         * @param message 服务端响应消息
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, CompleteMessage message) {
            int messageType = message.getPacketHeader().getMessageType();
            if (messageType == MessageType.LOGIN_MESSAGE.getType()) {
                handleLoginSuccess(ctx);
                return;
            }
            if (messageType == MessageType.HEARTBEAT_MESSAGE.getType()) {
                handlePong();
            }
        }

        /**
         * 首次收到登录响应后启动该连接的周期心跳。
         *
         * @param ctx Channel上下文
         */
        private void handleLoginSuccess(ChannelHandlerContext ctx) {
            if (loggedIn) {
                return;
            }
            loggedIn = true;
            LOGIN_SUCCESSES.incrementAndGet();
            sendHeartbeat(ctx);
            heartbeatFuture = ctx.executor().scheduleAtFixedRate(
                    () -> sendHeartbeat(ctx),
                    HEARTBEAT_INTERVAL_SECONDS,
                    HEARTBEAT_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );
        }

        /**
         * 发送心跳并记录开始时间。每条连接在一个心跳周期内最多只有一个待响应心跳。
         *
         * @param ctx Channel上下文
         */
        private void sendHeartbeat(ChannelHandlerContext ctx) {
            if (!ctx.channel().isActive() || STOPPING.get()) {
                return;
            }
            lastHeartbeatNanos = System.nanoTime();
            HEARTBEATS_SENT.increment();
            ctx.writeAndFlush(buildMessage(userId, MessageType.HEARTBEAT_MESSAGE, "ping"));
        }

        /**
         * 记录心跳响应数量及延迟。
         */
        private void handlePong() {
            PONGS_RECEIVED.increment();
            if (lastHeartbeatNanos == 0L) {
                return;
            }
            long latencyNanos = System.nanoTime() - lastHeartbeatNanos;
            PONG_LATENCY_NANOS.add(latencyNanos);
            updateMaxPongLatency(latencyNanos);
        }

        /**
         * 连接关闭时取消心跳并更新连接统计。
         *
         * @param ctx Channel上下文
         * @throws Exception 下游Handler处理异常
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
            }
            if (activeCounted) {
                ACTIVE_CONNECTIONS.decrementAndGet();
                if (!STOPPING.get()) {
                    DISCONNECTED_CONNECTIONS.incrementAndGet();
                }
                activeCounted = false;
            }
            super.channelInactive(ctx);
        }

        /**
         * 记录连接异常并关闭Channel。详细异常仅在debug级别输出，避免压测日志放大。
         *
         * @param ctx Channel上下文
         * @param cause 异常原因
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            CHANNEL_EXCEPTIONS.increment();
            log.debug("load test channel error, userId: {}", userId, cause);
            ctx.close();
        }
    }

    private ConnectionLoadTestClient() {
    }
}
