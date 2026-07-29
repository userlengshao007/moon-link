package com.moon.link.config;

import com.moon.link.cache.LinkClusterManager;

/**
 * 集群节点寻址配置入口。
 */
public final class ClusterConfig {

    /**
     * 查询指定节点对外提供的 gRPC 地址。
     *
     * @param machineId 节点机器 ID
     * @return gRPC 地址；节点不存在时返回 {@code null}
     */
    public static String getGrpcAddress(int machineId) {
        return LinkClusterManager.getGrpcAddress(machineId);
    }

    private ClusterConfig() {
    }
}
