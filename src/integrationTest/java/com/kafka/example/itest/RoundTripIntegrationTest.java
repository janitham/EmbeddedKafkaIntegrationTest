package com.kafka.example.itest;

import com.kafka.example.config.ReplyProducerConfig;
import com.kafka.example.config.RequestConsumerConfig;
import com.kafka.example.config.TopicConfig;
import com.kafka.example.model.Reply;
import com.kafka.example.model.Request;
import com.kafka.example.service.ReplyProducer;
import com.kafka.example.service.RequestConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@Import(RoundTripIntegrationTest.TestConfig.class)
class RoundTripIntegrationTest {

    @Autowired
    @Qualifier("requestTestKafkaTemplate")
    private KafkaTemplate<String, Request> requestTestKafkaTemplate;

    @Value("${topic.request}")
    private String requestTopicName;

    @Test
    public void embedded_broker_to_consume_request_test() throws Exception {
        requestTestKafkaTemplate.send(requestTopicName, new Request("Test", "Kafka!"));
        Thread.sleep(60000);
        assertEquals("Test", TestConfig.reply.getMsg());
        assertEquals("Kafka!", TestConfig.reply.getName());
    }

    @Configuration
    @Import({RequestConsumerConfig.class, ReplyProducerConfig.class, RequestConsumer.class, ReplyProducer.class, TopicConfig.class})
    public static class TestConfig {

        private static Reply reply;

        @Value(value = "${kafka.bootstrapAddress}")
        private String bootstrapAddress;

        @Value(value = "${topic.reply}")
        private String replyQueue;

        @Bean
        public ProducerFactory<String, Request> requestTestProducerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, Request> requestTestKafkaTemplate() {
            return new KafkaTemplate<>(requestTestProducerFactory());
        }

        @Bean
        public ConsumerFactory<String, Request> replyTestConsumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, replyQueue);
            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Request.class));
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Request> requestTestKafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, Request> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(replyTestConsumerFactory());
            return factory;
        }

        @KafkaListener(topics = "${topic.reply}", containerFactory = "requestTestKafkaListenerContainerFactory")
        public void greetingListener(final Reply reply) {
            TestConfig.reply = reply;
        }
    }
}
