package com.example.messagingdemo.serviceBus;

import com.example.messagingdemo.MenuOrder;
import com.example.messagingdemo.rabbitMq.RabbitMQSender;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/serviceBus")
public class ServiceBusDemoController {
    @Autowired
    ServiceBusProducer serviceBusProducer;

    @PostMapping(value = "/sender")
    public String producer(@RequestBody MenuOrder menuOrder) throws JsonProcessingException {
        serviceBusProducer.supplyMsg(menuOrder);
        return "Message sent to supplier";
    }
}
