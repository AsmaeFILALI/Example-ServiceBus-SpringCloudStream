package com.example.messagingdemo.serviceBus;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import com.example.messagingdemo.MenuOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

import static com.azure.spring.messaging.AzureHeaders.CHECKPOINTER;


@Slf4j
@Component
public class ServiceBusConsumer {


    @Autowired
    ActionService actionService;

    ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public Consumer<Message<String>> consume() {
        return message -> {
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(CHECKPOINTER);
            log.info("**********SERVICE_BUS ******** Message recieved : '{}'", message.getPayload());
            checkpointer.success()
                    .doOnSuccess(s -> {
                        log.info("Message '{}' successfully checkpointed", message.getPayload());
                        performActionOnOrder(message.getPayload());
                    })
                    .doOnError(e -> log.error("Error found", e))
                    .block();
        };
    }

    private void performActionOnOrder(String messagePayload) {
        try {
            MenuOrder menuOrder = objectMapper.readValue(messagePayload, MenuOrder.class);
            actionService.performAction(menuOrder);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
