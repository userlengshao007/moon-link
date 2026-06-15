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

public class NacosRegisterCenter {

    private NamingService namingService;

    public void init() throws Exception {
        namingService = NamingFactory.createNamingService(NacosRegisterConfig.PROPERTIES);

        Instance instance = new Instance();
        instance.setIp(System.getProperty("moon.link.host", "127.0.0.1"));
        instance.setPort(GrpcConfig.GRPC_PORT);
        instance.setServiceName(NacosRegisterConfig.SERVICE_NAME);

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

        List<Instance> instances = namingService.getAllInstances(
                NacosRegisterConfig.SERVICE_NAME,
                NacosRegisterConfig.DEFAULT_GROUP
        );

        listener.onInstancesChange(new HashSet<>(instances));
    }
}