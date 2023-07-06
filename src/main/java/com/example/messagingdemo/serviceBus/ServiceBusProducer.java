package com.example.messagingdemo.serviceBus;

import com.example.messagingdemo.MenuOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.function.Supplier;


@Slf4j
@Service
public class ServiceBusProducer {


    private static final Sinks.Many<Message<String>> many = Sinks.many().unicast().onBackpressureBuffer();
    ObjectMapper objectMapper = new ObjectMapper();


    @Bean
    public Supplier<Flux<Message<String>>> supply() {
        return ()->many.asFlux()
                .doOnNext(m->log.info("************* SERVICE_BUS **** Sending message {}", m))
                .doOnError(t->log.error("Error encountered", t));
    }

    public void supplyMsg(MenuOrder menuOrder) throws JsonProcessingException {
        many.emitNext(MessageBuilder.withPayload(objectMapper.writeValueAsString(menuOrder)).build(),Sinks.EmitFailureHandler.FAIL_FAST);
    }


}



