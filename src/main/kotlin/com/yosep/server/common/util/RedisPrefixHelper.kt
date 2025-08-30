package com.yosep.server.common.util

object RedisPrefixHelper {
    fun getRedisKey(prefix: String, vararg params: String?): String {
        val sb = StringBuilder(prefix)

        for (param in params) {
            sb.append(":")
            sb.append(param)
        }

        return sb.toString()
    }
}