package com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var kafkaDispatcher = new KafkaDispatcher();){
            for(int i = 0; i < 10; i++){
                System.out.println("Enviando mensagem " + i);
                var key = UUID.randomUUID().toString();
                var value = key + ";9023;88091;" + LocalDateTime.now().getMinute() + "_" + LocalDateTime.now().getSecond();
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you! ";
                kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL",key, email);
            }
        }
    }

    private static Callback getCallback() {
        return (sucess, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Mensagem Enviada " + sucess.topic() + " |partição " + sucess.partition() + " |offset " + sucess.offset());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
