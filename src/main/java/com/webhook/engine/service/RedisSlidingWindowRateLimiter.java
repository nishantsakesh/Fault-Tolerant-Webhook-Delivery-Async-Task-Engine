package com.webhook.engine.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;

@Service
@RequiredArgsConstructor
public class RedisSlidingWindowRateLimiter {

    private final StringRedisTemplate redisTemplate;

    // Lua script to atomically prune old timestamps and validate current count
    private static final String LUA_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])

        local clearBefore = now - window
        redis.call('ZREMRANGEBYSCORE', key, 0, clearBefore)

        local currentCount = redis.call('ZCARD', key)
        if currentCount < limit then
            redis.call('ZADD', key, now, now)
            redis.call('PEXPIRE', key, window)
            return 1
        else
            return 0
        end
    """;

    public boolean tryAcquire(String tenantId, int maxRps) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        long now = Instant.now().toEpochMilli();
        long windowMillis = 1000L;

        Long result = redisTemplate.execute(
            script,
            Collections.singletonList("rate:" + tenantId),
            String.valueOf(now),
            String.valueOf(windowMillis),
            String.valueOf(maxRps)
        );

        return result != null && result == 1L;
    }
}
