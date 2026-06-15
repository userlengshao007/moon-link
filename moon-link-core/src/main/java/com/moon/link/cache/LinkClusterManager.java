package com.moon.link.cache;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.moon.link.config.NacosRegisterConfig;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 链接集群管理器，负责管理集群中各机器的gRPC地址信息。
 * 通过Nacos注册中心获取实例信息，维护机器ID与gRPC地址的映射关系。
 */
public class LinkClusterManager {

    /**
     * 机器ID与gRPC地址的映射表，线程安全。
     * Key: 机器ID
     * Value: gRPC服务地址（格式：ip:port）
     */
    private static final Map<Integer, String> MACHINE_GRPC_ADDRESS_MAP = new ConcurrentHashMap<>();

    /**
     * 更新集群实例信息，从Nacos注册中心获取最新的服务实例列表，
     * 解析实例元数据中的机器ID和gRPC端口，构建机器ID到gRPC地址的映射。
     *
     * @param instances Nacos注册中心的服务实例集合，包含机器ID和gRPC端口等元数据
     */
    public static void updateInstances(Collection<Instance> instances) {
        MACHINE_GRPC_ADDRESS_MAP.clear();

        for (Instance instance : instances) {
            Map<String, String> metadata = instance.getMetadata();

            String machineIdText = metadata.get(NacosRegisterConfig.MACHINE_ID_KEY);
            String grpcPortText = metadata.get(NacosRegisterConfig.GRPC_PORT_KEY);

            // 跳过缺少必要元数据的实例
            if (machineIdText == null || grpcPortText == null) {
                continue;
            }

            int machineId = Integer.parseInt(machineIdText);
            String grpcAddress = instance.getIp() + ":" + grpcPortText;

            MACHINE_GRPC_ADDRESS_MAP.put(machineId, grpcAddress);
        }
    }

    /**
     * 根据机器ID获取对应的gRPC服务地址。
     *
     * @param machineId 机器ID
     * @return gRPC服务地址（格式：ip:port），如果机器ID不存在则返回null
     */
    public static String getGrpcAddress(int machineId) {
        return MACHINE_GRPC_ADDRESS_MAP.get(machineId);
    }
}