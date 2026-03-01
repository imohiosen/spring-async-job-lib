package com.imohiosen.asyncjob.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Example Spring Boot application demonstrating the async-job-starter library
 * with Postgres, Redis, and Kafka.
 */
@SpringBootApplication
public class AsyncJobExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncJobExampleApplication.class, args);
    }
}
