package com.moon.link.config;

public class ClusterConfig {

    // 先写死，未来打算用 Nacos 替换
    public static String getGrpcAddress(int machineId) {
        if (machineId == 1) {
            return "127.0.0.1:10000";
        }

        if (machineId == 2) {
            return "127.0.0.1:10001";
        }

        return null;
    }
}
