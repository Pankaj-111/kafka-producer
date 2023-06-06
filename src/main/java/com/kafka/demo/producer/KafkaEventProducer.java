package com.kafka.demo.producer;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.kafka.demo.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaEventProducer {
	@Autowired
	private KafkaTemplate<Integer, String> template;

	Gson gson = new Gson();

	public void publishEvent(LibraryEvent event) {
		final ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("library-events",
				event.getEventId(), gson.toJson(event));
		final CompletableFuture<SendResult<Integer, String>> future = template.send(record);
		future.thenAcceptAsync(e -> {
			log.info("Event details : {}", e);
		});
	}
}
