package com.kafka.example.service;

import com.kafka.example.model.Reply;
import com.kafka.example.model.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RequestConsumer {

    @Autowired
    private ReplyProducer replyProducer;

    @KafkaListener(topics = "${topic.request}", containerFactory = "requestKafkaListenerContainerFactory")
    public void greetingListener(final Request request) {
        replyProducer.send(new Reply(request.getMsg(),request.getName()));
    }
}
