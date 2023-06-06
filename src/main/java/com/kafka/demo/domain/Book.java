package com.kafka.demo.domain;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Book {
	private Integer bookId;
	private String bookName;
	private String author;
}
