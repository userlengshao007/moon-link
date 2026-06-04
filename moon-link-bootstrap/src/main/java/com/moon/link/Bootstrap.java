package com.moon.link;

import com.moon.link.netty.NettyServer;

public class Bootstrap {
    public static void main(String[] args) {
        new NettyServer(9999).start();
    }
}
