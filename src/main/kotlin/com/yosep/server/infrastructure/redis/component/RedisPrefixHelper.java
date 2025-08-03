package com.yosep.server.infrastructure.redis.component;

public class RedisPrefixHelper {

    public static String getRedisKey(String prefix, String... params) {
        StringBuilder sb = new StringBuilder(prefix);

        for (String param : params) {
            sb.append(":");
            sb.append(param);
        }

        return sb.toString();
    }
}
