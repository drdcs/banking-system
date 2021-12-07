package com.distributed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class Transactions {
    private String user;
    private double amount;
    private String transactionLocation;

    public Transactions(String user, double amount, String transactionLocation) {
        this.user = user;
        this.amount = amount;
        this.transactionLocation = transactionLocation;
    }

    public String getUser() {
        return user;
    }

    public double getAmount() {
        return amount;
    }

    public String getTransactionLocation() {
        return transactionLocation;
    }

    @Override
    public String toString() {
        return "Transactions{" +
                "user='" + user + '\'' +
                ", amount=" + amount +
                ", transactionLocation='" + transactionLocation + '\'' +
                '}';
    }

    public static class TransactionSerializer implements Serializer<Transactions> {

        @Override
        public byte[] serialize(String s, Transactions data) {
            byte[] serializedData = null;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                serializedData = objectMapper.writeValueAsString(data).getBytes();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return serializedData;
        }
    }
    
}
