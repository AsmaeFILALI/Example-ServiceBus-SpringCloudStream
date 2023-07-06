package com.example.messagingdemo.rabbitMq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.example.messagingdemo.MenuOrder;

@RestController
@RequestMapping(value = "/rabbitmq")
public class RabbitMQDemoController {
    @Autowired
    RabbitMQSender rabbitMQSender;

    @PostMapping(value = "/sender")
    public String producer(@RequestBody MenuOrder menuOrder) {
        rabbitMQSender.send(menuOrder);
        return "Message sent to the RabbitMQ Queue Successfully";
    }
}
