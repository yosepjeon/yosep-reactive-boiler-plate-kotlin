package com.yosep.server.common.component.ratelimit.local

import com.yosep.server.common.component.k8s.EndpointsProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import kotlin.math.max

@Component
class NodeCountBasedLimitCalculator(
    private val endpointsProvider: EndpointsProvider
) {

    @Value("\${cb.k8s.namespace:default}")
    private val namespace: String = "default"

    @Value("\${cb.k8s.service-name:cb-app}")
    private val serviceName: String = "cb-app"

    @Value("\${ratelimit.local.min-per-node:1}")
    private val minPerNode: Int = 1

    @Value("\${ratelimit.local.node-count-cache-ms:5000}")
    private val nodeCountCacheMs: Long = 5000

    @Volatile
    private var cachedNodeCount: Int = 1
    @Volatile
    private var lastNodeCountCheck: Long = 0

    fun calculatePerNodeLimit(totalLimit: Int): Int {
        val nodeCount = getNodeCount()
        val perNodeLimit = totalLimit / nodeCount
        return max(minPerNode, perNodeLimit)
    }

    fun getNodeCount(): Int {
        val now = System.currentTimeMillis()
        
        if (now - lastNodeCountCheck > nodeCountCacheMs) {
            refreshNodeCount()
            lastNodeCountCheck = now
        }
        
        return cachedNodeCount
    }

    private fun refreshNodeCount() {
        try {
            val endpoints = endpointsProvider.get(namespace, serviceName)
            val nodeCount = endpoints?.subsets?.sumOf { it.addresses.size } ?: 1
            cachedNodeCount = max(1, nodeCount)
        } catch (e: Exception) {
            cachedNodeCount = 1
        }
    }

    fun forceRefreshNodeCount(): Int {
        refreshNodeCount()
        lastNodeCountCheck = System.currentTimeMillis()
        return cachedNodeCount
    }
}