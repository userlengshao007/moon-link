package com.moon.link.grpc;

import com.moon.link.common.grpc.PushServiceGrpc;
import com.moon.link.config.ClusterConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点间 gRPC 客户端缓存，按机器 ID 复用 Channel 和 Stub。
 */
public final class GrpcClientManager {
    /** 机器 ID 到 gRPC Channel 的缓存，避免重复建立连接。 */
    private static final Map<Integer, ManagedChannel> CHANNEL_MAP = new ConcurrentHashMap<>();
    /** 机器 ID 到阻塞式 Stub 的线程安全缓存。 */
    private static final Map<Integer, PushServiceGrpc.PushServiceBlockingStub> STUB_MAP = new ConcurrentHashMap<>();

    /**
     * 获取指定机器的阻塞式gRPC存根
     * 
     * 使用缓存机制，同一机器ID只创建一个存根实例。
     * 如果缓存中不存在，则自动创建新的通道和存根。
     *
     * @param machineId 机器ID，用于定位目标gRPC服务
     * @return PushServiceBlockingStub 阻塞式gRPC存根，用于调用推送服务
     */
    public static PushServiceGrpc.PushServiceBlockingStub getBlockingStub(int machineId) {
        return STUB_MAP.computeIfAbsent(machineId, id ->
                PushServiceGrpc.newBlockingStub(getChannel(id))
        );
    }

    /**
     * 获取或创建指定机器的gRPC通道
     * 
     * 处理流程：
     * 1. 从缓存中查找通道，存在则直接返回
     * 2. 不存在则从集群配置中获取gRPC地址
     * 3. 解析地址并创建新的明文通道
     * 4. 将新通道放入缓存
     *
     * @param machineId 机器ID，用于查询对应的gRPC服务地址
     * @return ManagedChannel gRPC通信通道
     * @throws IllegalArgumentException 如果机器ID无效或配置中找不到对应地址
     */
    private static ManagedChannel getChannel(int machineId) {
        return CHANNEL_MAP.computeIfAbsent(machineId, id -> {
            // 从集群配置中获取gRPC服务地址
            String address = ClusterConfig.getGrpcAddress(id);

            // 地址无效，抛出异常
            if (address == null || address.isBlank()) {
                throw new IllegalArgumentException("unknown machineId: " + id);
            }

            // 解析地址格式 "host:port"
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // 创建gRPC通道，使用明文传输（不加密）
            return ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();
        });
    }

    private GrpcClientManager() {
    }
}
