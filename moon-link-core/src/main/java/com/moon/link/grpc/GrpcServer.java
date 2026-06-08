package com.moon.link.grpc;

import com.moon.link.config.GrpcConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcServer {
    private Server server;

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
