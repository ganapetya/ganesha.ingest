package com.ganesha.ingest.kafka;

import com.ganesha.ingest.page.kind.ArticlePage;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
@Log4j2
public class KafkaEndpoint {

    @Value("${kafka.topics.ingest-events}")
    private String ingestEventsTopic;

    private final Producer<String, ArticlePage> producer;

    public KafkaEndpoint(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.producer.acks}") String acks,
            @Value("${kafka.producer.retries}") int retries,
            @Value("${kafka.producer.linger-ms}") int lingerMs,
            @Value("${kafka.producer.key-serializer}") String keySerializer,
            @Value("${kafka.producer.value-serializer}") String valueSerializer) {
        
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("linger.ms", lingerMs);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        producer = new KafkaProducer<>(props);
    }

    public void sendArticlePage(ArticlePage page) {
        String key = UUID.randomUUID().toString();
        ProducerRecord<String, ArticlePage> record =
                new ProducerRecord<>(ingestEventsTopic, key, page);
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            String message = String.format("sent message to topic:%s partition:%s  offset:%s",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            log.info("Sent message to Kafka {}", message);
        } catch (InterruptedException e) {
            log.error("Error sending message to Kafka", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Error sending message to Kafka", e);
        }
    }
}
