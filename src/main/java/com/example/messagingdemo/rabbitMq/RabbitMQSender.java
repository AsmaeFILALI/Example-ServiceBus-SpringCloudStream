package com.example.messagingdemo.rabbitMq;

import com.example.messagingdemo.MenuOrder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
@Slf4j
@Service
public class RabbitMQSender {
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private Queue queue;

    public void send(MenuOrder menuOrder) {
        rabbitTemplate.convertAndSend(queue.getName(), menuOrder);
        log.info("******RABBITMQ ****** Sending Message to the Queue : " + menuOrder.toString());
    }
}