package com.moon.link.register;

import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.moon.link.config.GrpcConfig;
import com.moon.link.config.NacosRegisterConfig;
import com.moon.link.config.NettyConfig;
import com.moon.link.link.LinkConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Nacos 注册中心适配器，负责注册当前节点并监听集群成员变化。
 */
public class NacosRegisterCenter {

    /** Nacos 命名服务客户端。 */
    private NamingService namingService;

    /**
     * 创建命名服务客户端，并携带机器 ID、gRPC 端口和 Netty 端口注册当前实例。
     *
     * @throws Exception Nacos 客户端初始化或注册失败时抛出
     */
    public void init() throws Exception {
        namingService = NamingFactory.createNamingService(NacosRegisterConfig.PROPERTIES);

        Instance instance = new Instance();
        instance.setIp(System.getProperty("moon.link.host", "127.0.0.1"));
        instance.setPort(GrpcConfig.GRPC_PORT);
        instance.setServiceName(NacosRegisterConfig.SERVICE_NAME);

        // 元数据用于其他节点构建 machineId 到 gRPC 地址的路由表。
        Map<String, String> metadata = new HashMap<>();
        metadata.put(NacosRegisterConfig.MACHINE_ID_KEY, String.valueOf(LinkConfig.MACHINE_ID));
        metadata.put(NacosRegisterConfig.GRPC_PORT_KEY, String.valueOf(GrpcConfig.GRPC_PORT));
        metadata.put(NacosRegisterConfig.NETTY_PORT_KEY, String.valueOf(NettyConfig.NETTY_PORT));
        instance.setMetadata(metadata);

        namingService.registerInstance(
                NacosRegisterConfig.SERVICE_NAME,
                NacosRegisterConfig.DEFAULT_GROUP,
                instance
        );
    }

    /**
     * 订阅服务实例变化，并在订阅完成后主动发送一次当前全量实例。
     *
     * @param listener 集群实例监听器
     * @throws Exception 订阅或首次查询实例失败时抛出
     */
    public void subscribe(RegisterCenterListener listener) throws Exception {
        namingService.subscribe(
                NacosRegisterConfig.SERVICE_NAME,
                NacosRegisterConfig.DEFAULT_GROUP,
                event -> {
                    if (!(event instanceof NamingEvent)) {
                        return;
                    }

                    try {
                        List<Instance> instances = namingService.getAllInstances(
                                NacosRegisterConfig.SERVICE_NAME,
                                NacosRegisterConfig.DEFAULT_GROUP
                        );

                        listener.onInstancesChange(new HashSet<>(instances));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );

        // Nacos 订阅不会保证立即回调，因此主动加载一次，避免启动初期路由表为空。
        List<Instance> instances = namingService.getAllInstances(
                NacosRegisterConfig.SERVICE_NAME,
                NacosRegisterConfig.DEFAULT_GROUP
        );

        listener.onInstancesChange(new HashSet<>(instances));
    }
}
