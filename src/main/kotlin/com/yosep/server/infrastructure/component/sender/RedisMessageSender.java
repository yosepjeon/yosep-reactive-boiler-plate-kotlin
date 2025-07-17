package com.yosep.server.infrastructure.component.sender;

import org.redisson.api.RedissonReactiveClient;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class RedisMessageSender {

	private final RedissonReactiveClient redissonReactiveClient;

	public Mono<Long> send(String topic, String message) {
		return redissonReactiveClient.getTopic(topic).publish(message);
	}
}
