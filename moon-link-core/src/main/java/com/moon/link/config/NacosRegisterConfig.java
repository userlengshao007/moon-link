package com.moon.link.config;

import com.alibaba.nacos.api.PropertyKeyConst;

import java.util.Properties;

public class NacosRegisterConfig {
    public static final String SERVER_ADDR =
            System.getProperty("moon.link.nacos.server-addr", "127.0.0.1:8848");

    public static final String SERVICE_NAME =
            System.getProperty("moon.link.nacos.service-name", "moon-link");

    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    public static final String MACHINE_ID_KEY = "machine_id";
    public static final String GRPC_PORT_KEY = "grpc_port";
    public static final String NETTY_PORT_KEY = "netty_port";

    public static final Properties PROPERTIES = new Properties();

    static {
        PROPERTIES.put(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
    }
}