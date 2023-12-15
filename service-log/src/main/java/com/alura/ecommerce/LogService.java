package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args)  {
        var logService = new LogService();
        try (var kafkaService = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parser,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())); ){

            kafkaService.run();
        }
    }

    private void parser(ConsumerRecord<String, String> record) {
        System.out.println("============================");
        System.out.println("LOG " + record.topic() );
        System.out.println("KEY " + record.key());
        System.out.println("VALOR " + record.value());
        System.out.println("OFFSET " + record.offset());
        System.out.println("LOG processed");
    }
}
