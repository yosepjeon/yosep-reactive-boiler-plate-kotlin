//package com.yosep.server.infrastructure.component;
//
//import kotlin.reflect.jvm.internal.impl.builtins.functions.FunctionTypeKind.KSuspendFunction;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.redisson.api.*;
//import org.springframework.stereotype.Component;
//import org.springframework.util.ObjectUtils;
//import reactor.core.publisher.Mono;
//import java.io.Serializable;
//import java.util.concurrent.TimeUnit;
//
//@Component
//@Slf4j
//@RequiredArgsConstructor
//public class RedisCommandHelper {
//
//    private final RedissonReactiveClient redissonReactiveClient;
//
//    private RBucketReactive<Object> getRedisBucket(String key) {
//        return this.redissonReactiveClient.getBucket(key);
//    }
//
//    private RScoredSortedSetReactive<String> getZset(String key) {
//        return this.redissonReactiveClient.getScoredSortedSet(key);
//    }
//
//    public Mono<Object> get(String key) {
//        RBucketReactive<Object> temp = getRedisBucket(key);
//        return temp.get();
//    }
//
//    public Mono<Void> set(String key, Serializable value, long ttl) {
//        RBucketReactive<Object> temp = getRedisBucket(key);
//        return temp.set(value, ttl, TimeUnit.SECONDS);
//    }
//
//    public final Mono<Boolean> exists(String key) {
//        RBucketReactive<Object> temp = getRedisBucket(key);
//        return temp.isExists();
//    }
//
//    public final String type(String key) {
//        RType type = this.redissonReactiveClient.getKeys().getType(key).block();
//        if (type == null) {
//            return null;
//        }
//        return type.getClass().getName();
//    }
//
//    public Mono<Boolean> zAddByLock(String key, long score, String value) {
//
//        if (ObjectUtils.isEmpty(key)) {
//            return Mono.empty();
//        }
//
//        String lockName = key + ":lock";
//        final RLockReactive redissonClientLock = redissonReactiveClient.getLock(lockName);
//
//        return redissonClientLock.tryLock(3, 3, TimeUnit.SECONDS)
//            .flatMap(isLocked -> {
//                if (!isLocked) {
//                    return Mono.just(false);
//                }
//
//                RScoredSortedSetReactive<String> zSet = getZset(key);
//                return zSet.add(score, value);
//            })
//            .doFinally(signalType ->
//                redissonClientLock.isLocked()
//                    .flatMap(isLocked -> isLocked ? redissonClientLock.unlock() : Mono.empty()));
//    }
//
//
//}