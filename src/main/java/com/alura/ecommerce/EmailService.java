package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args)  {
        var emailService = new EmailService();
        var kafkaService = new KafkaService(EmailService.class.getSimpleName()
                ,"ECOMMERCE_SEND_EMAIL"
                ,emailService::parser);
        kafkaService.run();
    }

    private void parser(ConsumerRecord<String, String> record){
        System.out.println("============================");
        System.out.println("Processing Email");
        System.out.println("KEY " + record.key());
        System.out.println("VALOR " + record.value());
        System.out.println("OFFSET " + record.offset());
        System.out.println("Email processed");
    }

}
