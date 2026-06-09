package com.moon.link;

import com.moon.link.config.NettyConfig;
import com.moon.link.grpc.GrpcServer;
import com.moon.link.netty.NettyServer;

public class Bootstrap {
    public static void main(String[] args) {
        // 新增支持端口配置
        new Thread(() -> new NettyServer(NettyConfig.NETTY_PORT).start(), "netty-server").start();
        new GrpcServer().start();
    }
}
