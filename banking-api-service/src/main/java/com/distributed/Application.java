package com.distributed;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class Application
{
    /**
     * App serive is the main class where we define the topic for the valid and the suspicious topics.
     * The result is passed through the respective topics if the valid transaction location does not match.
     */

    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args )
    {
       Producer<String, Transactions> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
       try {
           processTransactions(new IncomingTransactionsReader(), new UserResidenceDatabase(), kafkaProducer);
       }catch(ExecutionException | InterruptedException ex) {
           ex.printStackTrace();
       } finally {
           kafkaProducer.flush();
           kafkaProducer.close();
       }
 }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           UserResidenceDatabase userResidenceDatabase,
                                           Producer<String, Transactions> kafkaProducer
                                           ) throws ExecutionException, InterruptedException {
        while(incomingTransactionsReader.hasNext()) {
            Transactions transaction = incomingTransactionsReader.next();
            String userResidence = userResidenceDatabase.getUserResidency(transaction.getUser());
            ProducerRecord<String, Transactions> record;
            //  producer record has topic, key and value pair...
            if(userResidence.equals(transaction.getTransactionLocation())){
                record =  new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC, transaction.getUser(), transaction);
            }else {
                record = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC, transaction.getUser(), transaction);
            }

            RecordMetadata recordMetadata = kafkaProducer.send(record).get();

            System.out.println(String.format("Record with (key %s , value %s), was sent to " +
                    ("partiton: %d, offset: %d, topics: %s"),
                    record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));

        }
    }


    private static Producer<String, Transactions> createKafkaProducer(String bootstrapServers) {

        /**
         * Producer takes parameter from properties file.
         */

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api-service");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transactions.TransactionSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
