package com.yosep.server.common.component.ratelimit.local

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

@Component
class LocalSlidingWindowRateLimiter {
    
    private val windowData = ConcurrentHashMap<String, WindowState>()
    
    data class WindowState(
        val windowSizeMs: Long,
        val requests: ConcurrentHashMap<Long, AtomicLong> = ConcurrentHashMap(),
        @Volatile var lastCleanupTime: Long = System.currentTimeMillis()
    )
    
    suspend fun tryAcquire(key: String, maxQps: Int, windowMs: Int): Boolean {
        val now = System.currentTimeMillis()
        val windowSizeMs = windowMs.toLong()
        
        val windowState = windowData.computeIfAbsent(key) { 
            WindowState(windowSizeMs) 
        }
        
        cleanupOldWindows(windowState, now)
        
        val currentWindowStart = now - (now % windowSizeMs)
        val previousWindowStart = currentWindowStart - windowSizeMs
        
        val currentWindow = windowState.requests.computeIfAbsent(currentWindowStart) { AtomicLong(0) }
        val previousWindow = windowState.requests[previousWindowStart]
        
        val currentCount = currentWindow.get()
        val previousCount = previousWindow?.get() ?: 0L
        
        val elapsedInCurrentWindow = now - currentWindowStart
        val weightOfPreviousWindow = max(0.0, 1.0 - (elapsedInCurrentWindow.toDouble() / windowSizeMs))
        
        val estimatedCount = currentCount + (previousCount * weightOfPreviousWindow).toLong()
        
        return if (estimatedCount < maxQps) {
            currentWindow.incrementAndGet()
            true
        } else {
            false
        }
    }
    
    private fun cleanupOldWindows(windowState: WindowState, now: Long) {
        if (now - windowState.lastCleanupTime < windowState.windowSizeMs) {
            return
        }
        
        val cutoff = now - (windowState.windowSizeMs * 2)
        windowState.requests.keys.removeIf { it < cutoff }
        windowState.lastCleanupTime = now
    }
    
    fun getCurrentCount(key: String): Long {
        val windowState = windowData[key] ?: return 0L
        val now = System.currentTimeMillis()
        val windowSizeMs = windowState.windowSizeMs
        
        val currentWindowStart = now - (now % windowSizeMs)
        val previousWindowStart = currentWindowStart - windowSizeMs
        
        val currentCount = windowState.requests[currentWindowStart]?.get() ?: 0L
        val previousCount = windowState.requests[previousWindowStart]?.get() ?: 0L
        
        val elapsedInCurrentWindow = now - currentWindowStart
        val weightOfPreviousWindow = max(0.0, 1.0 - (elapsedInCurrentWindow.toDouble() / windowSizeMs))
        
        return currentCount + (previousCount * weightOfPreviousWindow).toLong()
    }
    
    fun reset(key: String) {
        windowData.remove(key)
    }
}