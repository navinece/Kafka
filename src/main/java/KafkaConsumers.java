import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumers {

    public  static void main (String[] args ) {
        Properties properties = new Properties();
        //setting up Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //Setting up the De-serialiser to read the records from the topic
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        //Setting up the group id for the consumers
        properties.setProperty("group.id", "Feed1");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String,String> kafkaconsumer = new KafkaConsumer<String,String>(properties);
        kafkaconsumer.subscribe(Arrays.asList("email_opt_outs"));

        while (true)
        {
            ConsumerRecords<String,String> consumer_records = kafkaconsumer.poll(100);

            for ( ConsumerRecord<String,String> cr : consumer_records   ) {
                System.out.println ( " the key is " + cr.key());
                System.out.println ( " the value is " + cr.value());
                System.out.println ("the partition is " + cr.partition());
                System.out.println ("the offset  is " + cr.offset());
                System.out.println ("\n\n");



            }


        }




    }




}
