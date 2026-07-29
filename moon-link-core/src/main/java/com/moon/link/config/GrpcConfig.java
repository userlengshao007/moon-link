package com.moon.link.config;

/**
 * gRPC 服务配置。
 */
public final class GrpcConfig {
    /** gRPC 服务监听端口，可通过系统属性覆盖。 */
    public static final int GRPC_PORT = Integer.getInteger("moon.link.grpc.port", 10000);

    private GrpcConfig() {
    }
}
