package zerheri.fatima;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class TransactionProducer {

    // Kafka topic where transactions will be published
    private static final String TOPIC = "transactions-input";
    private static final Random RANDOM = new Random(); // Used to generate random transaction data
    private static final ObjectMapper MAPPER = new ObjectMapper(); // Used for converting objects to JSON strings

    public static void main(String[] args) {
        // Producer properties configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker address
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Key serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Value serializer

        // Creating a Kafka producer instance
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Infinite loop to continuously produce transactions
            while (true) {
                // Generate a random transaction
                Transaction transaction = generateTransaction();

                // Convert the transaction object to JSON format
                String json = MAPPER.writeValueAsString(transaction);

                // Create a producer record with the topic, key (userId), and value (transaction JSON)
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, transaction.getUserId(), json);

                // Send the record asynchronously
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message: " + exception.getMessage());
                    } else {
                        System.out.println("Sent transaction: " + json);
                    }
                });

                // Pause for 1 second before sending the next transaction
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates a random transaction for testing purposes.
     * @return A new Transaction object with random userId, amount, and timestamp.
     */
    private static Transaction generateTransaction() {
        // Generate a random user ID in the format "user_001" to "user_199"
        String userId = String.format("user_%03d", RANDOM.nextInt(200));

        // Generate a random amount between 1000 and 11000
        double amount = 1000 + RANDOM.nextDouble() * 10000;

        // Generate a timestamp in seconds (current system time)
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        // Return the generated Transaction object
        return new Transaction(userId, amount, timestamp);
    }
}

