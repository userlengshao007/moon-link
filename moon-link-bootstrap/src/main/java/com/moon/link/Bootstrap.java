package com.moon.link;

import com.moon.link.cache.LinkClusterManager;
import com.moon.link.config.NettyConfig;
import com.moon.link.grpc.GrpcServer;
import com.moon.link.link.LinkConfig;
import com.moon.link.netty.NettyServer;
import com.moon.link.redis.RedisClient;
import com.moon.link.register.NacosRegisterCenter;

/**
 * 应用程序启动入口类，负责初始化并启动整个 moon-link 服务。
 * 主要职责包括：初始化机器ID、注册Nacos服务发现、订阅集群实例变化、
 * 启动Netty服务器和gRPC服务器。
 */
public class Bootstrap {
    /**
     * 程序主入口方法，按顺序执行以下初始化流程：
     * 1. 初始化机器ID（从配置或Redis获取）
     * 2. 创建并初始化Nacos注册中心
     * 3. 订阅Nacos服务实例变化事件
     * 4. 在独立线程中启动Netty服务器
     * 5. 启动gRPC服务器（阻塞主线程）
     *
     * @param args 命令行参数数组
     * @throws Exception 初始化过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // 新增支持端口配置
        initMachineId();
        NacosRegisterCenter registerCenter = new NacosRegisterCenter();
        registerCenter.init();
        registerCenter.subscribe(LinkClusterManager::updateInstances);

        // 在独立线程中启动Netty服务器
        new Thread(() -> new NettyServer(NettyConfig.NETTY_PORT).start(), "netty-server").start();
        // 启动gRPC服务器（阻塞主线程）
        new GrpcServer().start();
    }

    /**
     * 初始化当前机器的ID。优先从系统属性"moon.link.machine.id"读取配置值，
     * 如果未配置或配置值无效（<=0），则通过Redis客户端自动生成唯一的机器ID。
     */
    private static void initMachineId() {
        int configuredMachineId = Integer.getInteger("moon.link.machine.id", 0);

        if (configuredMachineId > 0) {
            LinkConfig.MACHINE_ID = configuredMachineId;
        } else {
            LinkConfig.MACHINE_ID = RedisClient.generateMachineId();
        }
    }
}
