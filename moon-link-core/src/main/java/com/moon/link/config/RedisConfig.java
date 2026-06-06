package com.moon.link.config;

public class RedisConfig {
    public static final String REDIS_HOST = System.getProperty("moon.link.redis.host", "127.0.0.1");
    public static final int REDIS_PORT = Integer.getInteger("moon.link.redis.port", 6379);
    public static final String REDIS_PASSWORD = System.getProperty("moon.link.redis.password", "");
    public static final int REDIS_TIMEOUT_MILLIS = Integer.getInteger("moon.link.redis.timeoutMillis", 2000);
    public static final int ONLINE_EXPIRE_SECONDS = Integer.getInteger("moon.link.redis.onlineExpireSeconds", 300);

    public static final String MACHINE_ID_KEY = "moon-link:machine:id";
    public static final String PREFIX_USER_ID = "moon-link:user:";

    private RedisConfig() {
    }
}
