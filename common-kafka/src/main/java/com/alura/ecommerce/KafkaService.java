package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parser;
    public KafkaService(String groupId, String topico, ConsumerFunction parser, Class<T> type, Map<String, String> properties) {
        this.consumer = new KafkaConsumer<String, String>(getProperties(groupId, type, properties));
        this.parser = parser;
        consumer.subscribe(Collections.singletonList(topico));
    }

    public KafkaService(String groupId, Pattern topico, ConsumerFunction parser, Class<T> type, Map<String, String> properties) {
        this.consumer = new KafkaConsumer<String, String>(getProperties(groupId, type, properties));
        this.parser = parser;
        consumer.subscribe(topico);
    }

    private Properties getProperties(String groupId, Class<T> type, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    public void run() {
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Encontrou " + records.count());
                for (var record : records){
                    parser.consume(record);
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
