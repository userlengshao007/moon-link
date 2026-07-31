package com.moon.link.heartbeat;

import com.moon.link.config.RedisConfig;
import com.moon.link.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 在线路由批量续期管理器。
 * <p>
 * Netty 心跳线程只记录本地活跃用户，独立定时线程负责去重、分批并通过 Redis Pipeline 续期，
 * 避免同步 Redis 操作阻塞 Netty EventLoop。
 */
@Slf4j
public final class OnlineRenewManager {
    private static final Object ACTIVE_USER_LOCK = new Object();
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final ScheduledThreadPoolExecutor RENEW_EXECUTOR =
            new ScheduledThreadPoolExecutor(1, new RenewThreadFactory());

    private static Set<Long> activeUserIds = new HashSet<>();

    static {
        RENEW_EXECUTOR.setRemoveOnCancelPolicy(true);
    }

    /**
     * 启动在线路由定时续期任务。
     */
    public static void start() {
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        validateConfig();
        RENEW_EXECUTOR.scheduleAtFixedRate(
                OnlineRenewManager::flushSafely,
                RedisConfig.ONLINE_RENEW_INTERVAL_SECONDS,
                RedisConfig.ONLINE_RENEW_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
        log.info("online renew task started, intervalSeconds: {}, batchSize: {}",
                RedisConfig.ONLINE_RENEW_INTERVAL_SECONDS, RedisConfig.ONLINE_RENEW_BATCH_SIZE);
    }

    /**
     * 标记用户最近产生过有效心跳。
     *
     * @param userId 已完成登录认证的用户 ID
     */
    public static void markActive(long userId) {
        synchronized (ACTIVE_USER_LOCK) {
            activeUserIds.add(userId);
        }
    }

    /**
     * 停止定时任务并丢弃尚未提交的续期标记。
     * <p>
     * 网关即将下线时不能继续延长在线路由 TTL；连接路由应由连接关闭流程主动删除，
     * 异常情况下再由 Redis TTL 兜底。
     */
    public static void shutdown() {
        if (!STARTED.compareAndSet(true, false)) {
            return;
        }

        RENEW_EXECUTOR.shutdown();
        try {
            if (!RENEW_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                RENEW_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RENEW_EXECUTOR.shutdownNow();
        }
        drainActiveUserIds();
    }

    private static void validateConfig() {
        if (RedisConfig.ONLINE_RENEW_INTERVAL_SECONDS <= 0) {
            throw new IllegalArgumentException("online renew interval must be greater than zero");
        }
        if (RedisConfig.ONLINE_RENEW_BATCH_SIZE <= 0) {
            throw new IllegalArgumentException("online renew batch size must be greater than zero");
        }
        if (RedisConfig.ONLINE_RENEW_INTERVAL_SECONDS >= RedisConfig.ONLINE_EXPIRE_SECONDS) {
            throw new IllegalArgumentException("online renew interval must be less than online expire time");
        }
    }

    private static void flushSafely() {
        try {
            flushActiveUsers();
        } catch (RuntimeException e) {
            log.error("flush active users failed", e);
        }
    }

    private static void flushActiveUsers() {
        Set<Long> pendingUserIds = drainActiveUserIds();
        if (pendingUserIds.isEmpty()) {
            return;
        }

        List<Long> userIds = new ArrayList<>(pendingUserIds);
        int batchSize = RedisConfig.ONLINE_RENEW_BATCH_SIZE;
        int successCount = 0;

        for (int fromIndex = 0; fromIndex < userIds.size(); fromIndex += batchSize) {
            int toIndex = Math.min(fromIndex + batchSize, userIds.size());
            List<Long> batchUserIds = userIds.subList(fromIndex, toIndex);
            if (RedisClient.batchExpireUserOnline(batchUserIds)) {
                successCount += batchUserIds.size();
            } else {
                restoreActiveUsers(batchUserIds);
            }
        }

        log.info("online routes renewed, totalCount: {}, successCount: {}, retryCount: {}",
                userIds.size(), successCount, userIds.size() - successCount);
    }

    private static Set<Long> drainActiveUserIds() {
        synchronized (ACTIVE_USER_LOCK) {
            Set<Long> pendingUserIds = activeUserIds;
            activeUserIds = new HashSet<>();
            return pendingUserIds;
        }
    }

    private static void restoreActiveUsers(List<Long> userIds) {
        synchronized (ACTIVE_USER_LOCK) {
            activeUserIds.addAll(userIds);
        }
    }

    private OnlineRenewManager() {
    }

    /**
     * 在线续期线程工厂。
     */
    private static final class RenewThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "online-renew-worker");
            thread.setDaemon(true);
            return thread;
        }
    }
}
