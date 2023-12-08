package com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args)  {
        var fraudDetectorService = new FraudDetectorService();

        try(var kafkaService = new KafkaService(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parser)){
            kafkaService.run();
        }
    }

    private void parser(ConsumerRecord<String, String> record) {
        System.out.println("============================");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("KEY " + record.key());
        System.out.println("VALOR " + record.value());
        System.out.println("OFFSET " + record.offset());
        System.out.println("PARTITION " + record.partition());
//        System.out.println("PROCESSASNDO " + i + " : " + records.count());
        System.out.println("Order processed");
    }

}
