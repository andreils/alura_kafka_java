package com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>();){
            try (var emailDispatcher = new KafkaDispatcher<Email>();){
                for(int i = 0; i < 5; i++){
                    System.out.println("Enviando mensagem " + i);
                    var orderKey = UUID.randomUUID().toString();
                    var userKey = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 500 + 1);
                    var order = new Order(orderKey, userKey, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", orderKey, order);

                    var email = new Email("email@mail.com", "Thank You, your order has processed");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL",orderKey, email);
                }
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
