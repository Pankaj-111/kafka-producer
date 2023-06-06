package com.kafka.demo.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.demo.domain.Book;
import com.kafka.demo.domain.EventType;
import com.kafka.demo.domain.LibraryEvent;
import com.kafka.demo.producer.KafkaEventProducer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class LibraryController {
	
	private static final int EVENT_ID = -99;

	private static final int BOOK_ID = 1000;

	private static final String AUTHOR_NAME = "Test Author";

	private static final String BOOK_NAME = "Test Book";

	@Autowired
	KafkaEventProducer eventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> addBook(@RequestBody final LibraryEvent event) {
		event.setEventType(EventType.NEW);
		eventProducer.publishEvent(event);
		log.info("***************** Library Event Sent sucessfully : {}", event);
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
	}
	
	
	@GetMapping("/v1/test")
	public void test() {
		List<LibraryEvent> list = new ArrayList<>();
		for (int i=0; i<1; i++) {
			list.add(this.getLibraryEvent(i));
		}
		list.parallelStream().forEach(e-> eventProducer.publishEvent(e));
	}
	
	private LibraryEvent getLibraryEvent(int i) {
		final Book book = createBook();
		final LibraryEvent event = LibraryEvent.builder()
				.eventId(EVENT_ID)
				.book(book)
				.build();
		return event;
	}

	private Book createBook() {
		final Book book = Book.builder()
				.bookId(BOOK_ID)
				.bookName(BOOK_NAME)
				.author(AUTHOR_NAME)
				.build();
		return book;
	}
}
