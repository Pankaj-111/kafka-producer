package com.kafka.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.google.gson.Gson;
import com.kafka.demo.domain.Book;
import com.kafka.demo.domain.EventType;
import com.kafka.demo.domain.LibraryEvent;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 1, brokerProperties = {
		"listeners=PLAINTEXT://localhost:9092", "port=9092" })
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })
@Timeout(5)
public class LibraryEventIntegTest {

	private static final String CONSUMER_GROUP = "grp1";

	private static final int EVENT_ID = 100;

	private static final int BOOK_ID = 1000;

	private static final String AUTHOR_NAME = "Test Author";

	private static final String BOOK_NAME = "Test Book";
	
	private Consumer<Integer, String> consumer;
	
	private Gson gson;

	@Autowired
	private TestRestTemplate restTemplate;
	
	@Autowired
	private EmbeddedKafkaBroker broker;

	@Test
	@Timeout(5)
	public void postEventTest() {
		final ResponseEntity<LibraryEvent> respone = restTemplate
				.postForEntity("/v1/libraryevent", getLibraryEvent(),
				LibraryEvent.class);
		assertEquals(HttpStatus.CREATED, respone.getStatusCode());
		if (respone.getStatusCode() == HttpStatus.CREATED) {
			assertEquals(EVENT_ID, respone.getBody().getEventId());
			assertEquals(BOOK_NAME, respone.getBody().getBook().getBookName());
			assertEquals(AUTHOR_NAME, respone.getBody().getBook().getAuthor());
		}
		final ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, "library-events");
		assertEquals(100, record.key());
		
		final String value = record.value();
		final LibraryEvent event = gson.fromJson(value, LibraryEvent.class);
		assertEquals(EVENT_ID, event.getEventId());
		assertEquals(EventType.NEW, event.getEventType());
		assertEquals(BOOK_ID, event.getBook().getBookId());
		assertEquals(BOOK_NAME, event.getBook().getBookName());
		assertEquals(AUTHOR_NAME, event.getBook().getAuthor());
	}
	
	@BeforeEach
	public void setup() {
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps(CONSUMER_GROUP, "true", broker));
		consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs, new IntegerDeserializer(),
				new StringDeserializer()).createConsumer();
		broker.consumeFromAllEmbeddedTopics(consumer);
		gson = new Gson();
	}
	
	@AfterEach
	public void tearDown() {
		consumer.close();
	}

	private LibraryEvent getLibraryEvent() {
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
