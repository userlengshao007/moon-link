package com.moon.link.config;

/**
 * Redis 连接及业务键配置。
 */
public final class RedisConfig {
    /** Redis 服务地址。 */
    public static final String REDIS_HOST = System.getProperty("moon.link.redis.host", "127.0.0.1");
    /** Redis 服务端口。 */
    public static final int REDIS_PORT = Integer.getInteger("moon.link.redis.port", 6379);
    /** Redis 认证密码，空字符串表示不认证。 */
    public static final String REDIS_PASSWORD = System.getProperty("moon.link.redis.password", "");
    /** Redis 连接超时时间，单位为毫秒。 */
    public static final int REDIS_TIMEOUT_MILLIS = Integer.getInteger("moon.link.redis.timeoutMillis", 2000);
    /** 用户在线状态的过期时间，单位为秒。 */
    public static final int ONLINE_EXPIRE_SECONDS = Integer.getInteger("moon.link.redis.onlineExpireSeconds", 300);
    /** 在线用户批量续期任务执行间隔，单位为秒。 */
    public static final int ONLINE_RENEW_INTERVAL_SECONDS =
            Integer.getInteger("moon.link.redis.onlineRenewIntervalSeconds", 90);
    /** 单次 Redis Pipeline 包含的最大续期命令数。 */
    public static final int ONLINE_RENEW_BATCH_SIZE =
            Integer.getInteger("moon.link.redis.onlineRenewBatchSize", 1000);

    /** 分配节点机器 ID 使用的自增键。 */
    public static final String MACHINE_ID_KEY = "moon-link:machine:id";
    /** 用户在线路由键前缀。 */
    public static final String PREFIX_USER_ID = "moon-link:user:";

    private RedisConfig() {
    }
}
