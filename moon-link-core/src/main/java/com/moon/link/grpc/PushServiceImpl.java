package com.moon.link.grpc;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import com.moon.link.link.LinkConfig;
import com.moon.link.redis.RedisClient;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 推送服务实现类
 * <p>
 * 实现gRPC推送服务，提供向用户推送消息的功能。
 * 通过Netty Channel将消息推送到目标用户的WebSocket连接。
 */
@Slf4j
public class PushServiceImpl extends PushServiceGrpc.PushServiceImplBase {
    /**
     * 向指定用户推送消息
     * <p>
     * 处理流程：
     * 1. 检查目标用户是否在线（是否存在Channel上下文）
     * 2. 检查Channel是否处于活跃状态
     * 3. 构建推送消息并发送
     * 4. 根据发送结果返回成功或失败响应
     *
     * @param request          推送请求，包含目标用户ID和消息内容
     * @param responseObserver gRPC响应观察者，用于异步返回推送结果
     */
    @Override
    public void push2User(PushGrpc.Push2UserRequest request,
                          StreamObserver<PushGrpc.Push2UserResponse> responseObserver) {

        long toId = request.getToId();

        // 获取目标用户的Channel上下文
        ChannelHandlerContext ctx = UserChannelCtxMap.get(toId);

        // 用户不在线，去查询
        if (ctx == null) {
            Integer targetMachineId = RedisClient.getMachineId(toId);

            if (targetMachineId == null) {
                responseObserver.onNext(buildResponse(
                        PushGrpc.ResponseCode.USER_OFFLINE,
                        false,
                        "user offline"
                ));
                responseObserver.onCompleted();
                return;
            }

            if (targetMachineId == LinkConfig.MACHINE_ID) {
                responseObserver.onNext(buildResponse(
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "user channel not found in current machine"
                ));
                responseObserver.onCompleted();
                return;
            }

            try {
                PushGrpc.Push2UserResponse response = GrpcClientManager
                        .getBlockingStub(targetMachineId)
                        .push2User(request);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("forward push failed, toId: {}, targetMachineId: {}", toId, targetMachineId, e);

                responseObserver.onNext(buildResponse(
                        PushGrpc.ResponseCode.INTERNAL_ERROR,
                        false,
                        "forward push failed"
                ));
                responseObserver.onCompleted();
            }

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

    /**
     * 发送信息给指定多个用户
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void push2Users(PushGrpc.Push2UsersRequest request, StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        List<Long> toIds = request.getToIdsList();

        if (toIds == null || toIds.isEmpty()) {
            responseObserver.onNext(PushGrpc.Push2UsersResponse.newBuilder()
                    .setTotal(0)
                    .setSuccessCount(0)
                    .setFailCount(0)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        // 使用线程安全的列表存储推送结果
        List<PushGrpc.PushResult> results = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, List<Long>> remoteMachineUsers = new HashMap<>();
        List<LocalPushTask> localPushTasks = new ArrayList<>();
        List<Long> needQueryRedisUserIds = new ArrayList<>();

        // 第一轮只判断本机内存里的 channel。
        // 本机存在活跃 channel 的用户可以直接推送；本机找不到的用户先收集起来，
        // 后面统一用 Redis Pipeline 批量查询用户所在机器，避免循环里逐个访问 Redis。
        for (Long toId : toIds) {
            ChannelHandlerContext ctx = UserChannelCtxMap.get(toId);

            if (ctx == null) {
                needQueryRedisUserIds.add(toId);
                continue;
            }

            if (!ctx.channel().isActive()) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "channel inactive"
                ));

                continue;
            }

            localPushTasks.add(new LocalPushTask(toId, ctx));
        }

        // 第二轮批量查询 Redis，把不在本机内存中的用户分成：
        // 1. Redis 查不到：用户离线
        // 2. Redis 显示在当前机器：本机 channel 丢失或不可用
        // 3. Redis 显示在其他机器：按目标机器分组，后面通过 gRPC 批量转发
        List<Integer> machineIds = RedisClient.batchGetMachineId(needQueryRedisUserIds);
        for (int i = 0; i < needQueryRedisUserIds.size(); i++) {
            Long toId = needQueryRedisUserIds.get(i);
            Integer targetMachineId = machineIds.get(i);

            if (targetMachineId == null) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.USER_OFFLINE,
                        false,
                        "user offline"
                ));
                continue;
            }

            if (targetMachineId == LinkConfig.MACHINE_ID) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "user channel not found in current machine"
                ));
                continue;
            }

            remoteMachineUsers
                    .computeIfAbsent(targetMachineId, id -> new ArrayList<>())
                    .add(toId);
        }

        int asyncTaskCount = localPushTasks.size() + remoteMachineUsers.size();
        if (asyncTaskCount == 0) {
            responseObserver.onNext(buildBatchResponse(results));
            responseObserver.onCompleted();
            return;
        }

        AtomicInteger remaining = new AtomicInteger(asyncTaskCount);

        for (LocalPushTask task : localPushTasks) {
            CompleteMessage pushMessage = buildPushMessage(task.toId, request.getMessage());

            task.ctx.writeAndFlush(pushMessage).addListener(future -> {
                // 根据发送结果记录成功或失败
                if (future.isSuccess()) {
                    results.add(buildPushResult(
                            task.toId,
                            PushGrpc.ResponseCode.SUCCESS,
                            true,
                            "push success"
                    ));
                } else {
                    results.add(buildPushResult(
                            task.toId,
                            PushGrpc.ResponseCode.INTERNAL_ERROR,
                            false,
                            "push failed"
                    ));
                }

                finishOne(responseObserver, results, remaining);
            });
        }

        for (Map.Entry<Integer, List<Long>> entry : remoteMachineUsers.entrySet()) {
            int targetMachineId = entry.getKey();
            List<Long> remoteUserIds = entry.getValue();

            PushGrpc.Push2UsersRequest forwardRequest = PushGrpc.Push2UsersRequest.newBuilder()
                    .addAllToIds(remoteUserIds)
                    .setMessage(request.getMessage())
                    .build();

            try {
                PushGrpc.Push2UsersResponse forwardResponse = GrpcClientManager
                        .getBlockingStub(targetMachineId)
                        .push2Users(forwardRequest);

                results.addAll(forwardResponse.getResultsList());
            } catch (Exception e) {
                log.error("forward batch push failed, targetMachineId: {}, userIds: {}",
                        targetMachineId, remoteUserIds, e);

                for (Long toId : remoteUserIds) {
                    results.add(buildPushResult(
                            toId,
                            PushGrpc.ResponseCode.INTERNAL_ERROR,
                            false,
                            "forward batch push failed"
                    ));
                }
            }

            finishOne(responseObserver, results, remaining);
        }
    }

    /**
     * 完成一个用户的推送后检查是否全部完成
     * 
     * 使用原子计数器递减，当所有推送都完成后才返回响应。
     *
     * @param responseObserver gRPC响应观察者
     * @param results 所有推送结果列表
     * @param remaining 剩余未完成推送数量的原子计数器
     */
    private void finishOne(StreamObserver<PushGrpc.Push2UsersResponse> responseObserver,
                           List<PushGrpc.PushResult> results,
                           AtomicInteger remaining) {
        if (remaining.decrementAndGet() == 0) {
            responseObserver.onNext(buildBatchResponse(results));
            responseObserver.onCompleted();
        }
    }

    private CompleteMessage buildPushMessage(PushGrpc.Push2UserRequest request) {
        return buildPushMessage(request.getToId(), request.getMessage());
    }

    /**
     * 构建单个用户的推送结果
     *
     * @param toId   接收者用户ID
     * @param code   响应状态码
     * @param success 是否推送成功
     * @param msg    结果描述信息
     * @return PushResult 构建完成的推送结果对象
     */
    private PushGrpc.PushResult buildPushResult(long toId,
                                                PushGrpc.ResponseCode code,
                                                boolean success,
                                                String msg) {
        return PushGrpc.PushResult.newBuilder()
                .setToId(toId)
                .setCode(code)
                .setSuccess(success)
                .setMsg(msg)
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

    /**
     * 构建批量推送响应
     * 
     * 统计成功和失败数量，组装完整的批量响应结果。
     *
     * @param results 所有用户的推送结果列表
     * @return Push2UsersResponse 构建完成的批量推送响应对象
     */
    private PushGrpc.Push2UsersResponse buildBatchResponse(List<PushGrpc.PushResult> results) {
        // 统计成功推送数量
        int successCount = 0;

        for (PushGrpc.PushResult result : results) {
            if (result.getSuccess()) {
                successCount++;
            }
        }

        // 计算总数和失败数
        int total = results.size();
        int failCount = total - successCount;

        return PushGrpc.Push2UsersResponse.newBuilder()
                .setTotal(total)
                .setSuccessCount(successCount)
                .setFailCount(failCount)
                .addAllResults(results)
                .build();
    }

    /**
     * 构建完整的推送消息
     * 
     * 根据目标用户ID和消息体构建包含消息头和消息体的完整消息。
     *
     * @param toId    接收者用户ID
     * @param message 原始消息体
     * @return CompleteMessage 构建完成的完整消息对象
     */
    private CompleteMessage buildPushMessage(long toId, PushGrpc.PushMessageBody message) {
        return CompleteMessage.newBuilder()
                // 设置消息头
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(toId)
                        .setMessageType(message.getMessageType())
                        .build())
                // 设置消息体
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(message.getFromUserId())
                        .setToId(toId)
                        .setMessageType(message.getMessageType())
                        .setContent(message.getContent())
                        .setTimeStamp(message.getTimeStamp())
                        .build())
                .build();
    }

    private static class LocalPushTask {
        private final long toId;
        private final ChannelHandlerContext ctx;

        private LocalPushTask(long toId, ChannelHandlerContext ctx) {
            this.toId = toId;
            this.ctx = ctx;
        }
    }
}
