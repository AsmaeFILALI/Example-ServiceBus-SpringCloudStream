package com.example.messagingdemo.rabbitMq;

import com.example.messagingdemo.MenuOrder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RabbitListener(queues = "rabbitmq.queue", id = "listener")
public class RabbitMQReceiver {

    @RabbitHandler
    public void receiver(MenuOrder menuOrder) {
        log.info("********RABBITMQ************* Message recieved : " + menuOrder.toString());
    }
}