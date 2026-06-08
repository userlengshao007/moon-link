package com.moon.link;

import com.moon.link.grpc.GrpcServer;
import com.moon.link.netty.NettyServer;

public class Bootstrap {
    public static void main(String[] args) {
        new Thread(() -> new NettyServer(9999).start(), "netty-server").start();
        new GrpcServer().start();
    }
}
