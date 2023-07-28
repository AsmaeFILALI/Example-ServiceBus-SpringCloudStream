package com.example.messagingdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration(exclude = { org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.class })
public class MessagingdemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(MessagingdemoApplication.class, args);
	}

}
