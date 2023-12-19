package com.alura.ecommerce;

import java.math.BigDecimal;

public class Order {

    private final String orderKey;
    private final String userKey;
    private final BigDecimal amount;

    public Order(String orderKey, String userKey, BigDecimal amount) {
        this.orderKey = orderKey;
        this.userKey = userKey;
        this.amount = amount;
    }
}
