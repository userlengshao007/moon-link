package com.moon.link.config;

import com.moon.link.cache.LinkClusterManager;

public class ClusterConfig {

    public static String getGrpcAddress(int machineId) {
        return LinkClusterManager.getGrpcAddress(machineId);
    }
}