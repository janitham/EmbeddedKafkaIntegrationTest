package com.kafka.example.service;

import com.kafka.example.model.Reply;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ReplyProducer {

    private final KafkaTemplate<String, Reply> replyKafkaTemplate;

    @Value("${topic.reply}")
    private String replyTopicName;

    public ReplyProducer(@Qualifier("replyKafkaTemplate") KafkaTemplate<String, Reply> replyKafkaTemplate) {
        this.replyKafkaTemplate = replyKafkaTemplate;
    }

    public void send(Reply reply) {
        replyKafkaTemplate.send(replyTopicName, reply);
    }

}
