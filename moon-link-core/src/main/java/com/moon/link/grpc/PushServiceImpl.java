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
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 推送服务。
 *
 * <p>{@code Push2User(s)} 是业务层调用的路由接口，负责定位用户所在节点并允许一次重路由；
 * {@code PushLocalUser(s)} 是节点间调用的本地接口，只访问当前节点的 Channel Map。</p>
 */
@Slf4j
public class PushServiceImpl extends PushServiceGrpc.PushServiceImplBase {

    /**
     * 路由并推送给单个用户。
     */
    @Override
    public void push2User(PushGrpc.Push2UserRequest request,
                          StreamObserver<PushGrpc.Push2UserResponse> responseObserver) {
        ChannelHandlerContext localContext = UserChannelCtxMap.get(request.getToId());
        if (localContext != null && localContext.channel().isActive()) {
            pushLocalUser(request, responseObserver);
            return;
        }

        routeSingleUser(request, responseObserver);
    }

    /**
     * 仅向当前节点上的用户推送，不查询 Redis，也不向其他节点转发。
     */
    @Override
    public void pushLocalUser(PushGrpc.Push2UserRequest request,
                              StreamObserver<PushGrpc.Push2UserResponse> responseObserver) {
        long toId = request.getToId();
        ChannelHandlerContext context = UserChannelCtxMap.get(toId);
        if (context == null || !context.channel().isActive()) {
            complete(responseObserver, buildResponse(
                    PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                    false,
                    "user channel not found or inactive in current machine"
            ));
            return;
        }

        context.writeAndFlush(buildPushMessage(toId, request.getMessage())).addListener(future -> {
            if (future.isSuccess()) {
                complete(responseObserver, buildResponse(
                        PushGrpc.ResponseCode.SUCCESS,
                        true,
                        "push success"
                ));
                return;
            }

            log.error("push local message failed, toId: {}", toId, future.cause());
            complete(responseObserver, buildResponse(
                    PushGrpc.ResponseCode.INTERNAL_ERROR,
                    false,
                    "push failed"
            ));
        });
    }

    /**
     * 路由并批量推送给多个用户。
     */
    @Override
    public void push2Users(PushGrpc.Push2UsersRequest request,
                           StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        List<Long> toIds = request.getToIdsList();
        if (toIds.isEmpty()) {
            complete(responseObserver, buildBatchResponse(Collections.emptyList()));
            return;
        }

        List<PushGrpc.PushResult> results = Collections.synchronizedList(new ArrayList<>());
        RoutingPlan routingPlan = buildInitialRoutingPlan(toIds, results);
        int taskCount = routingPlan.localTasks.size() + routingPlan.remoteMachineUsers.size();
        if (taskCount == 0) {
            complete(responseObserver, buildBatchResponse(results));
            return;
        }

        AtomicInteger remaining = new AtomicInteger(taskCount);
        scheduleLocalTasks(
                routingPlan.localTasks,
                request.getMessage(),
                results,
                remaining,
                responseObserver
        );

        for (Map.Entry<Integer, List<Long>> entry : routingPlan.remoteMachineUsers.entrySet()) {
            forwardLocalBatchWithRetry(
                    entry.getKey(),
                    entry.getValue(),
                    request.getMessage(),
                    results,
                    remaining,
                    responseObserver
            );
        }
    }

    /**
     * 仅向当前节点上的多个用户推送，不查询 Redis，也不向其他节点转发。
     */
    @Override
    public void pushLocalUsers(PushGrpc.Push2UsersRequest request,
                               StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        List<PushGrpc.PushResult> results = Collections.synchronizedList(new ArrayList<>());
        List<LocalPushTask> localTasks = new ArrayList<>();

        for (Long toId : request.getToIdsList()) {
            ChannelHandlerContext context = UserChannelCtxMap.get(toId);
            if (context == null || !context.channel().isActive()) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "user channel not found or inactive in current machine"
                ));
                continue;
            }
            localTasks.add(new LocalPushTask(toId, context));
        }

        if (localTasks.isEmpty()) {
            complete(responseObserver, buildBatchResponse(results));
            return;
        }

        scheduleLocalTasks(
                localTasks,
                request.getMessage(),
                results,
                new AtomicInteger(localTasks.size()),
                responseObserver
        );
    }

    private void routeSingleUser(PushGrpc.Push2UserRequest request,
                                 StreamObserver<PushGrpc.Push2UserResponse> responseObserver) {
        long toId = request.getToId();
        Integer targetMachineId = RedisClient.getMachineId(toId);
        if (targetMachineId == null) {
            complete(responseObserver, buildResponse(
                    PushGrpc.ResponseCode.USER_OFFLINE,
                    false,
                    "user offline"
            ));
            return;
        }

        if (targetMachineId == LinkConfig.MACHINE_ID) {
            pushLocalUser(request, responseObserver);
            return;
        }

        PushGrpc.Push2UserResponse firstResponse = forwardLocalUser(request, targetMachineId);
        if (firstResponse.getCode() != PushGrpc.ResponseCode.CHANNEL_INACTIVE) {
            complete(responseObserver, firstResponse);
            return;
        }

        // 目标节点本地 Channel 已失效时，只允许入口节点重新查 Redis 并重路由一次。
        Integer retryMachineId = RedisClient.getMachineId(toId);
        if (retryMachineId == null) {
            complete(responseObserver, buildResponse(
                    PushGrpc.ResponseCode.USER_OFFLINE,
                    false,
                    "user offline after reroute"
            ));
            return;
        }

        if (retryMachineId == LinkConfig.MACHINE_ID) {
            pushLocalUser(request, responseObserver);
            return;
        }

        if (retryMachineId.equals(targetMachineId)) {
            complete(responseObserver, firstResponse);
            return;
        }

        complete(responseObserver, forwardLocalUser(request, retryMachineId));
    }

    private PushGrpc.Push2UserResponse forwardLocalUser(PushGrpc.Push2UserRequest request,
                                                        int targetMachineId) {
        try {
            return GrpcClientManager.getBlockingStub(targetMachineId).pushLocalUser(request);
        } catch (Exception e) {
            log.error("forward local push failed, toId: {}, targetMachineId: {}",
                    request.getToId(), targetMachineId, e);
            return buildResponse(
                    PushGrpc.ResponseCode.INTERNAL_ERROR,
                    false,
                    "forward local push failed"
            );
        }
    }

    private RoutingPlan buildInitialRoutingPlan(List<Long> toIds,
                                                List<PushGrpc.PushResult> results) {
        RoutingPlan plan = new RoutingPlan();
        List<Long> needQueryRedisUserIds = new ArrayList<>();

        for (Long toId : toIds) {
            ChannelHandlerContext context = UserChannelCtxMap.get(toId);
            if (context == null) {
                needQueryRedisUserIds.add(toId);
                continue;
            }
            if (!context.channel().isActive()) {
                needQueryRedisUserIds.add(toId);
                continue;
            }
            plan.localTasks.add(new LocalPushTask(toId, context));
        }

        List<Integer> machineIds = RedisClient.batchGetMachineId(needQueryRedisUserIds);
        for (int i = 0; i < needQueryRedisUserIds.size(); i++) {
            long toId = needQueryRedisUserIds.get(i);
            Integer targetMachineId = machineIds.get(i);
            if (targetMachineId == null) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.USER_OFFLINE,
                        false,
                        "user offline"
                ));
            } else if (targetMachineId == LinkConfig.MACHINE_ID) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "user channel not found in current machine"
                ));
            } else {
                plan.remoteMachineUsers
                        .computeIfAbsent(targetMachineId, key -> new ArrayList<>())
                        .add(toId);
            }
        }
        return plan;
    }

    private void forwardLocalBatchWithRetry(int targetMachineId,
                                            List<Long> userIds,
                                            PushGrpc.PushMessageBody message,
                                            List<PushGrpc.PushResult> results,
                                            AtomicInteger remaining,
                                            StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        PushGrpc.Push2UsersResponse response = forwardLocalUsers(targetMachineId, userIds, message);
        List<Long> inactiveUserIds = new ArrayList<>();
        for (PushGrpc.PushResult result : response.getResultsList()) {
            if (result.getCode() == PushGrpc.ResponseCode.CHANNEL_INACTIVE) {
                inactiveUserIds.add(result.getToId());
            } else {
                results.add(result);
            }
        }

        if (!inactiveUserIds.isEmpty()) {
            scheduleBatchReroute(
                    targetMachineId,
                    inactiveUserIds,
                    message,
                    results,
                    remaining,
                    responseObserver
            );
        }
        finishOne(responseObserver, results, remaining);
    }

    private void scheduleBatchReroute(int previousMachineId,
                                      List<Long> userIds,
                                      PushGrpc.PushMessageBody message,
                                      List<PushGrpc.PushResult> results,
                                      AtomicInteger remaining,
                                      StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        RoutingPlan retryPlan = new RoutingPlan();
        List<Integer> retryMachineIds = RedisClient.batchGetMachineId(userIds);

        for (int i = 0; i < userIds.size(); i++) {
            long toId = userIds.get(i);
            Integer retryMachineId = retryMachineIds.get(i);
            if (retryMachineId == null) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.USER_OFFLINE,
                        false,
                        "user offline after reroute"
                ));
                continue;
            }

            if (retryMachineId == LinkConfig.MACHINE_ID) {
                ChannelHandlerContext context = UserChannelCtxMap.get(toId);
                if (context != null && context.channel().isActive()) {
                    retryPlan.localTasks.add(new LocalPushTask(toId, context));
                } else {
                    results.add(buildPushResult(
                            toId,
                            PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                            false,
                            "user channel not found in current machine"
                    ));
                }
                continue;
            }

            if (retryMachineId == previousMachineId) {
                results.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.CHANNEL_INACTIVE,
                        false,
                        "user channel still inactive in target machine"
                ));
                continue;
            }

            retryPlan.remoteMachineUsers
                    .computeIfAbsent(retryMachineId, key -> new ArrayList<>())
                    .add(toId);
        }

        int retryTaskCount = retryPlan.localTasks.size() + retryPlan.remoteMachineUsers.size();
        remaining.addAndGet(retryTaskCount);
        scheduleLocalTasks(
                retryPlan.localTasks,
                message,
                results,
                remaining,
                responseObserver
        );

        for (Map.Entry<Integer, List<Long>> entry : retryPlan.remoteMachineUsers.entrySet()) {
            PushGrpc.Push2UsersResponse retryResponse =
                    forwardLocalUsers(entry.getKey(), entry.getValue(), message);
            results.addAll(retryResponse.getResultsList());
            finishOne(responseObserver, results, remaining);
        }
    }

    private PushGrpc.Push2UsersResponse forwardLocalUsers(int targetMachineId,
                                                          List<Long> userIds,
                                                          PushGrpc.PushMessageBody message) {
        PushGrpc.Push2UsersRequest request = PushGrpc.Push2UsersRequest.newBuilder()
                .addAllToIds(userIds)
                .setMessage(message)
                .build();
        try {
            return GrpcClientManager.getBlockingStub(targetMachineId).pushLocalUsers(request);
        } catch (Exception e) {
            log.error("forward local batch push failed, targetMachineId: {}, userIds: {}",
                    targetMachineId, userIds, e);
            List<PushGrpc.PushResult> failedResults = new ArrayList<>();
            for (Long toId : userIds) {
                failedResults.add(buildPushResult(
                        toId,
                        PushGrpc.ResponseCode.INTERNAL_ERROR,
                        false,
                        "forward local batch push failed"
                ));
            }
            return buildBatchResponse(failedResults);
        }
    }

    private void scheduleLocalTasks(List<LocalPushTask> tasks,
                                    PushGrpc.PushMessageBody message,
                                    List<PushGrpc.PushResult> results,
                                    AtomicInteger remaining,
                                    StreamObserver<PushGrpc.Push2UsersResponse> responseObserver) {
        for (LocalPushTask task : tasks) {
            task.context.writeAndFlush(buildPushMessage(task.toId, message)).addListener(future -> {
                if (future.isSuccess()) {
                    results.add(buildPushResult(
                            task.toId,
                            PushGrpc.ResponseCode.SUCCESS,
                            true,
                            "push success"
                    ));
                } else {
                    log.error("push local message failed, toId: {}", task.toId, future.cause());
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
    }

    private void finishOne(StreamObserver<PushGrpc.Push2UsersResponse> responseObserver,
                           List<PushGrpc.PushResult> results,
                           AtomicInteger remaining) {
        if (remaining.decrementAndGet() == 0) {
            complete(responseObserver, buildBatchResponse(results));
        }
    }

    private PushGrpc.Push2UserResponse buildResponse(PushGrpc.ResponseCode code,
                                                     boolean success,
                                                     String message) {
        return PushGrpc.Push2UserResponse.newBuilder()
                .setCode(code)
                .setSuccess(success)
                .setMsg(message)
                .build();
    }

    private PushGrpc.PushResult buildPushResult(long toId,
                                                PushGrpc.ResponseCode code,
                                                boolean success,
                                                String message) {
        return PushGrpc.PushResult.newBuilder()
                .setToId(toId)
                .setCode(code)
                .setSuccess(success)
                .setMsg(message)
                .build();
    }

    private PushGrpc.Push2UsersResponse buildBatchResponse(List<PushGrpc.PushResult> results) {
        int successCount = 0;
        for (PushGrpc.PushResult result : results) {
            if (result.getSuccess()) {
                successCount++;
            }
        }
        return PushGrpc.Push2UsersResponse.newBuilder()
                .setTotal(results.size())
                .setSuccessCount(successCount)
                .setFailCount(results.size() - successCount)
                .addAllResults(results)
                .build();
    }

    private CompleteMessage buildPushMessage(long toId, PushGrpc.PushMessageBody message) {
        return CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setUid(toId)
                        .setMessageType(message.getMessageType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(message.getFromUserId())
                        .setToId(toId)
                        .setMessageType(message.getMessageType())
                        .setContent(message.getContent())
                        .setTimeStamp(message.getTimeStamp())
                        .build())
                .build();
    }

    private <T> void complete(StreamObserver<T> responseObserver, T response) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static final class LocalPushTask {
        private final long toId;
        private final ChannelHandlerContext context;

        private LocalPushTask(long toId, ChannelHandlerContext context) {
            this.toId = toId;
            this.context = context;
        }
    }

    private static final class RoutingPlan {
        private final List<LocalPushTask> localTasks = new ArrayList<>();
        private final Map<Integer, List<Long>> remoteMachineUsers = new HashMap<>();
    }
}
