package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {

    public static void main(String[] args)  {
        var consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Encontrou " + records.count());
                var i = 1;
                for (var record : records){
                    System.out.println("============================");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println("KEY " + record.key());
                    System.out.println("VALOR " + record.value());
                    System.out.println("OFFSET " + record.offset());
                    System.out.println("PARTITION " + record.partition());
                    System.out.println("PROCESSASNDO " + i + " : " + records.count());
                    System.out.println("Order processed");

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        return properties;
    }
}
