package consumer.distributed;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {


    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args) {
        String consumerGroup = "account-manager";
        System.out.println("Consumer is part of consumer group " + consumerGroup);
        Consumer<String, Transaction> transactionConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessage(VALID_TRANSACTIONS_TOPIC, transactionConsumer);
    }


    public static void consumeMessage(String topic, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords  = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            for(ConsumerRecord<String, Transaction> record: consumerRecords) {
                System.out.println(String.format("Received record (key: %s, value: %s, partition: %d, offset: %d",
                        record.key(), record.value(), record.partition(), record.offset()));
                approveTransaction(record.value());
            }
        }
    }

    private static void approveTransaction(Transaction transaction) {
        // Business logic for approval
        System.out.println(String.format("Authorizing transaction for user %s, in the amount of $%.2f",
                transaction.getUser(), transaction.getAmount()));
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);

    }
}
