package com.moon.link.config;

public class GrpcConfig {
    public static final int GRPC_PORT = Integer.getInteger("moon.link.grpc.port", 10000);
}
