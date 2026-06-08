package com.moon.link.grpc;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * 推送服务实现类
 * 
 * 实现gRPC推送服务，提供向用户推送消息的功能。
 * 通过Netty Channel将消息推送到目标用户的WebSocket连接。
 */
@Slf4j
public class PushServiceImpl extends PushServiceGrpc.PushServiceImplBase {
    /**
     * 向指定用户推送消息
     * 
     * 处理流程：
     * 1. 检查目标用户是否在线（是否存在Channel上下文）
     * 2. 检查Channel是否处于活跃状态
     * 3. 构建推送消息并发送
     * 4. 根据发送结果返回成功或失败响应
     *
     * @param request 推送请求，包含目标用户ID和消息内容
     * @param responseObserver gRPC响应观察者，用于异步返回推送结果
     */
    @Override
    public void push2User(PushGrpc.Push2UserRequest request,
                          StreamObserver<PushGrpc.Push2UserResponse> responseObserver) {

        long toId = request.getToId();

        // 获取目标用户的Channel上下文
        ChannelHandlerContext ctx = UserChannelCtxMap.get(toId);

        // 用户不在线，返回离线错误
        if (ctx == null) {
            // 发送一个响应消息给客户端
            responseObserver.onNext(buildResponse(
                    PushGrpc.ResponseCode.USER_OFFLINE,
                    false,
                    "user offline"
            ));
            // 标记响应流结束，必须调用
            responseObserver.onCompleted();
            return;
        }

        // Channel不可用，返回通道错误
        if (!ctx.channel().isActive()) {
            responseObserver.onNext(buildResponse(
                    PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                    false,
                    "channel inactive"
            ));
            responseObserver.onCompleted();
            return;
        }

        // 构建完整的推送消息
        CompleteMessage pushMessage = buildPushMessage(request);

        // 异步发送消息到客户端
        ChannelFuture future = ctx.writeAndFlush(pushMessage);

        // 监听发送结果并返回响应
        future.addListener(f -> {
            if (f.isSuccess()) {
                responseObserver.onNext(buildResponse(
                        PushGrpc.ResponseCode.SUCCESS,
                        true,
                        "push success"
                ));
            } else {
                log.error("push message failed, toId: {}", toId, f.cause());
                responseObserver.onNext(buildResponse(
                        PushGrpc.ResponseCode.INTERNAL_ERROR,
                        false,
                        "push failed"
                ));
            }

            responseObserver.onCompleted();
        });
    }

    private CompleteMessage buildPushMessage(PushGrpc.Push2UserRequest request) {
        PushGrpc.PushMessageBody message = request.getMessage();

        return CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(request.getToId())
                        .setMessageType(message.getMessageType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(message.getFromUserId())
                        .setToId(request.getToId())
                        .setMessageType(message.getMessageType())
                        .setContent(message.getContent())
                        .setTimeStamp(message.getTimeStamp())
                        .build())
                .build();
    }

    private PushGrpc.Push2UserResponse buildResponse(PushGrpc.ResponseCode code,
                                                     boolean success,
                                                     String msg) {
        return PushGrpc.Push2UserResponse.newBuilder()
                .setCode(code)
                .setSuccess(success)
                .setMsg(msg)
                .build();
    }
}
