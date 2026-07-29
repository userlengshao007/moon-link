package com.moon.link.grpc;

import com.moon.link.config.GrpcConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * 节点间消息推送使用的 gRPC 服务端。
 */
@Slf4j
public class GrpcServer {
    /** gRPC 服务实例。 */
    private Server server;

    /**
     * 启动推送服务并阻塞当前线程，直到服务终止。
     */
    public void start() {
        try {
            server = ServerBuilder.forPort(GrpcConfig.GRPC_PORT)
                    .addService(new PushServiceImpl())
                    .build()
                    .start();

            log.info("GrpcServer started on port {}", GrpcConfig.GRPC_PORT);

            server.awaitTermination();
        } catch (Exception e) {
            log.error("GrpcServer start failed", e);
        }
    }
}
