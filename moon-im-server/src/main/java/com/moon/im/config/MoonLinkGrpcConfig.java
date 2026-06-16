package com.moon.im.config;

import com.moon.link.common.grpc.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Moon-Link gRPC客户端配置类，负责创建和管理与moon-link服务的gRPC通信组件。
 * 通过Spring Bean管理gRPC通道和服务存根的生命周期，支持依赖注入。
 */
@Configuration
public class MoonLinkGrpcConfig {

    /**
     * 创建并配置连接到moon-link服务的gRPC ManagedChannel。
     * 使用明文传输（非TLS），在Bean销毁时自动关闭连接。
     *
     * @param host moon-link服务的gRPC主机地址，默认值为127.0.0.1
     * @param port moon-link服务的gRPC端口号，默认值为10000
     * @return 配置完成的ManagedChannel实例，用于建立gRPC连接
     */
    @Bean(destroyMethod = "shutdownNow")
    public ManagedChannel moonLinkManagedChannel(
            @Value("${moon-link.grpc.host:127.0.0.1}") String host,
            @Value("${moon-link.grpc.port:10000}") int port) {
        return ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }

    /**
     * 创建PushService的阻塞式gRPC服务存根，用于调用moon-link的推送服务。
     * 基于已配置的ManagedChannel构建，支持同步调用方式。
     *
     * @param moonLinkManagedChannel gRPC通道，由Spring容器注入
     * @return PushServiceBlockingStub实例，用于执行推送相关的gRPC调用
     */
    @Bean
    public PushServiceGrpc.PushServiceBlockingStub pushServiceBlockingStub(ManagedChannel moonLinkManagedChannel) {
        return PushServiceGrpc.newBlockingStub(moonLinkManagedChannel);
    }
}
