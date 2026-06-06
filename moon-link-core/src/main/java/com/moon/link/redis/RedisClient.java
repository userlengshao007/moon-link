package com.moon.link.redis;

import com.moon.link.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

@Slf4j
public class RedisClient {
    // 用 jedis 来操作 不用 redisTemplate，因为 RedisTemplate 是依赖 SpringBoot 的
    public static final JedisPool JEDIS_POOL = buildPool();

    /**
     * 构建Jedis连接池
     * 
     * 创建并配置JedisPool实例，设置连接池参数和认证信息。
     * 根据是否配置密码来决定使用哪种构造函数创建连接池。
     *
     * @return JedisPool 配置好的Jedis连接池实例
     */
    private static JedisPool buildPool() {
        // 配置连接池参数
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 设置连接池中最大连接数为64
        poolConfig.setMaxTotal(64);
        // 设置连接池中最大空闲连接数为16
        poolConfig.setMaxIdle(16);
        // 设置连接池中最小空闲连接数为4
        poolConfig.setMinIdle(4);
        // 设置从连接池获取连接时的最大等待时间为500毫秒
        poolConfig.setMaxWait(Duration.ofMillis(500));
        // 设置在从连接池借用连接时进行有效性检查
        poolConfig.setTestOnBorrow(true);

        // 根据是否配置密码来创建连接池
        if (RedisConfig.REDIS_PASSWORD == null || RedisConfig.REDIS_PASSWORD.isBlank()) {
            return new JedisPool(poolConfig, RedisConfig.REDIS_HOST, RedisConfig.REDIS_PORT,
                    RedisConfig.REDIS_TIMEOUT_MILLIS);
        }

        return new JedisPool(poolConfig, RedisConfig.REDIS_HOST, RedisConfig.REDIS_PORT,
                RedisConfig.REDIS_TIMEOUT_MILLIS, RedisConfig.REDIS_PASSWORD);
    }
    public static int generateMachineId() {
        try (Jedis jedis = JEDIS_POOL.getResource()) {
            Long id = jedis.incr(RedisConfig.MACHINE_ID_KEY);
            return id.intValue();
        } catch (Exception e) {
            log.warn("[Redis] generate machine id failed, fallback to local machineId=1, error: {}", e.getMessage());
            return 1;
        }
    }

    public static boolean setUserOnline(long userId, int machineId) {
        try (Jedis jedis = JEDIS_POOL.getResource()) {
            jedis.setex(userKey(userId), RedisConfig.ONLINE_EXPIRE_SECONDS, String.valueOf(machineId));
            return true;
        } catch (Exception e) {
            log.warn("[Redis] set user online failed, userId: {}, machineId: {}, error: {}",
                    userId, machineId, e.getMessage());
            return false;
        }
    }

    public static boolean expireUserOnline(long userId) {
        try (Jedis jedis = JEDIS_POOL.getResource()) {
            return jedis.expire(userKey(userId), RedisConfig.ONLINE_EXPIRE_SECONDS) == 1;
        } catch (Exception e) {
            log.warn("[Redis] expire user online failed, userId: {}, error: {}", userId, e.getMessage());
            return false;
        }
    }

    public static boolean removeUserOnline(long userId) {
        try (Jedis jedis = JEDIS_POOL.getResource()) {
            jedis.del(userKey(userId));
            return true;
        } catch (Exception e) {
            log.warn("[Redis] remove user online failed, userId: {}, error: {}", userId, e.getMessage());
            return false;
        }
    }

    public static Integer getMachineId(long userId) {
        try (Jedis jedis = JEDIS_POOL.getResource()) {
            String machineId = jedis.get(userKey(userId));
            return machineId == null ? null : Integer.parseInt(machineId);
        } catch (Exception e) {
            log.warn("[Redis] get machine id failed, userId: {}, error: {}", userId, e.getMessage());
            return null;
        }
    }

    private static String userKey(long userId) {
        return RedisConfig.PREFIX_USER_ID + userId;
    }


}
