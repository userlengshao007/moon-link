package com.moon.link.redis;

import com.moon.link.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    /**
     * 批量查询用户所在机器ID。
     * <p>
     * 批量推送时如果逐个调用 Redis GET，1000 个用户就会产生 1000 次网络往返。
     * Pipeline 会把多个 GET 命令一次性发给 Redis，再统一读取结果，从而减少网络 IO。
     * 返回结果的顺序和 userIds 的顺序保持一致，方便调用方按下标对应用户。
     *
     * @param userIds 用户ID列表
     * @return 每个用户对应的机器ID；用户不在线或查询失败时对应位置为 null
     */
    public static List<Integer> batchGetMachineId(List<Long> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Collections.emptyList();
        }

        try (Jedis jedis = JEDIS_POOL.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            List<Response<String>> responses = new ArrayList<>(userIds.size());

            // 先把所有 GET 命令放进 pipeline，这里只是排队，不会立即等待每条命令返回。
            for (Long userId : userIds) {
                responses.add(pipeline.get(userKey(userId)));
            }

            // sync 会把 pipeline 中排队的命令批量发送给 Redis，并一次性取回结果。
            pipeline.sync();

            List<Integer> machineIds = new ArrayList<>(responses.size());
            for (Response<String> response : responses) {
                String machineId = response.get();
                machineIds.add(machineId == null ? null : Integer.parseInt(machineId));
            }

            return machineIds;
        } catch (Exception e) {
            log.warn("[Redis] batch get machine id failed, size: {}, error: {}",
                    userIds.size(), e.getMessage());

            // 出现异常时保持返回列表长度一致，调用方仍然可以按下标处理为离线。
            return new ArrayList<>(Collections.nCopies(userIds.size(), null));
        }
    }

    private static String userKey(long userId) {
        return RedisConfig.PREFIX_USER_ID + userId;
    }


}
