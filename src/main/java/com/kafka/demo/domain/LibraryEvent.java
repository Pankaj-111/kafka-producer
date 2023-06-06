package com.kafka.demo.domain;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class LibraryEvent {
	private EventType eventType;
	private Integer eventId;
	private Book book;
}
