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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * 协议测试客户端
 * <p>
 * 用于测试消息协议的编解码器和消息处理流程。
 * 模拟客户端与服务器建立连接，发送登录请求和心跳消息。
 */
@Slf4j
public class ProtocolTestClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = Integer.getInteger("moon.link.netty.port", 9999);
    private static final long TEST_UID = Long.getLong("moon.link.test.uid", 10001L);
    private static final long TEST_TO_UID = Long.getLong("moon.link.test.to-id", 10002L);
    private static final boolean SEND_PRIVATE_CHAT =
            Boolean.getBoolean("moon.link.test.send-private-chat");
    private static final int HEARTBEAT_INTERVAL_SECONDS =
            Integer.getInteger("moon.link.test.heartbeat-interval-seconds", 30);

    /**
     * 客户端启动入口
     * <p>
     * 创建Netty客户端并连接到服务器，配置编解码器和消息处理器。
     *
     * @param args 命令行参数
     * @throws InterruptedException 中断异常
     */
    public static void main(String[] args) throws InterruptedException {
        // 创建事件循环组，用于处理IO操作
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            // 配置客户端引导类
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 按顺序添加处理器：解码器 -> 编码器 -> 业务处理器
                            ch.pipeline()
                                    .addLast(new MessageProtocolDecoder())
                                    .addLast(new MessageProtocolEncoder())
                                    .addLast(new ClientMessageHandler());
                        }
                    });

            // 连接服务器并等待连接完成
            ChannelFuture future = bootstrap.connect(HOST, PORT).sync();
            log.info("ProtocolTestClient connected to {}:{}", HOST, PORT);
            // 阻塞等待通道关闭
            future.channel().closeFuture().sync();
        } finally {
            // 优雅关闭事件循环组
            group.shutdownGracefully();
        }
    }

    /**
     * 客户端消息处理器
     * <p>
     * 处理与服务器的消息交互流程：
     * 1. 连接建立后发送登录请求
     * 2. 接收登录响应后发送心跳消息
     * 3. 接收心跳响应后关闭连接
     */
    private static class ClientMessageHandler extends SimpleChannelInboundHandler<CompleteMessage> {

        private ScheduledFuture<?> heartbeatFuture;
        private boolean privateChatSent;

        /**
         * 连接激活时触发，发送登录请求
         *
         * @param ctx 通道上下文
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            CompleteMessage loginMessage = buildMessage(MessageType.LOGIN_MESSAGE, "login request");
            log.info("client send login, uid: {}", TEST_UID);
            ctx.writeAndFlush(loginMessage);
        }

        /**
         * 接收到服务器响应消息
         * <p>
         * 根据消息类型进行不同处理：
         * - 登录响应：发送心跳消息
         * - 心跳响应：关闭连接，测试结束
         *
         * @param ctx 通道上下文
         * @param msg 接收到的完整消息
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, CompleteMessage msg) {
            int messageType = msg.getPacketHeader().getMessageType();
            String content = msg.getPacketBody().getContent();
            log.info("client receive response, type: {}, content: {}", messageType, content);

            if (messageType == MessageType.LOGIN_MESSAGE.getType()) {
                startHeartbeat(ctx);
                return;
            }

            if (messageType == MessageType.HEARTBEAT_MESSAGE.getType()) {
                log.info("client receive heartbeat pong, keep online, uid: {}", TEST_UID);

                if (SEND_PRIVATE_CHAT && !privateChatSent) {
                    privateChatSent = true;
                    CompleteMessage privateChatMessage =
                            buildPrivateChatMessage("hello private chat from netty client");

                    log.info("client send private chat, from: {}, to: {}", TEST_UID, TEST_TO_UID);
                    ctx.writeAndFlush(privateChatMessage);
                }
            }
        }

        /**
         * 登录成功后启动定时心跳。
         * <p>
         * 这里使用当前 Channel 所属的 EventLoop 定时执行，不额外创建线程。
         * 只要客户端持续发送心跳，服务端的 IdleStateHandler 就不会误判连接读空闲。
         */
        private void startHeartbeat(ChannelHandlerContext ctx) {
            if (HEARTBEAT_INTERVAL_SECONDS <= 0) {
                log.info("client heartbeat disabled, uid: {}", TEST_UID);
                return;
            }

            if (heartbeatFuture != null && !heartbeatFuture.isCancelled()) {
                return;
            }

            sendHeartbeat(ctx);
            heartbeatFuture = ctx.executor().scheduleAtFixedRate(
                    () -> sendHeartbeat(ctx),
                    HEARTBEAT_INTERVAL_SECONDS,
                    HEARTBEAT_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );

            log.info("client heartbeat scheduled, uid: {}, intervalSeconds: {}",
                    TEST_UID, HEARTBEAT_INTERVAL_SECONDS);
        }

        private void sendHeartbeat(ChannelHandlerContext ctx) {
            if (!ctx.channel().isActive()) {
                return;
            }

            CompleteMessage heartBeatMessage = buildMessage(MessageType.HEARTBEAT_MESSAGE, "ping");
            log.info("client send heartbeat, uid: {}", TEST_UID);
            ctx.writeAndFlush(heartBeatMessage);
        }

        /**
         * 连接关闭时取消定时任务，避免 channel 已经断开后还继续提交心跳写入。
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
            }
            super.channelInactive(ctx);
        }

        /**
         * 捕获异常并关闭连接
         *
         * @param ctx   通道上下文
         * @param cause 异常信息
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("client error", cause);
            ctx.close();
        }

        /**
         * 构建完整的消息对象
         * <p>
         * 构造包含消息头（用户ID、Token等）和消息体（内容、时间戳等）的完整消息。
         *
         * @param messageType 消息类型（登录、心跳等）
         * @param content     消息内容
         * @return CompleteMessage 构建完成的完整消息对象
         */
        private CompleteMessage buildMessage(MessageType messageType, String content) {
            return CompleteMessage.newBuilder()
                    // 设置消息头信息
                    .setPacketHeader(PacketHeader.newBuilder()
                            .setAppId(1)
                            .setUid(TEST_UID)
                            .setToken("test-token")
                            .setCompression(0)
                            .setEncryption(0)
                            .setMessageType(messageType.getType())
                            .build())
                    // 设置消息体信息
                    .setPacketBody(PacketBody.newBuilder()
                            .setFromUserId(TEST_UID)
                            .setTimeStamp(System.currentTimeMillis())
                            .setMessageType(messageType.getType())
                            .setContent(content)
                            .build())
                    .build();
        }

        private CompleteMessage buildPrivateChatMessage(String content) {
            return CompleteMessage.newBuilder()
                    .setPacketHeader(PacketHeader.newBuilder()
                            .setAppId(1)
                            .setUid(TEST_UID)
                            .setToken("test-token")
                            .setCompression(0)
                            .setEncryption(0)
                            .setMessageType(MessageType.PRIVATE_CHAT_MESSAGE.getType())
                            .build())
                    .setPacketBody(PacketBody.newBuilder()
                            .setFromUserId(TEST_UID)
                            .setToId(TEST_TO_UID)
                            .setTimeStamp(System.currentTimeMillis())
                            .setMessageType(MessageType.PRIVATE_CHAT_MESSAGE.getType())
                            .setContent(content)
                            .build())
                    .build();
        }
    }
}
