package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args){
        var emailService = new EmailService();
        try (var kafkaService = new KafkaService(EmailService.class.getSimpleName()
                ,"ECOMMERCE_SEND_EMAIL"
                ,emailService::parser); ){

            kafkaService.run();
        }
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
