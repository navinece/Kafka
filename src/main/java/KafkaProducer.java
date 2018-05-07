import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {


    public  static void main (String[] args ) {

        Properties properties = new Properties();

        //setting up Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //producer acknowledgements
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> producer_record = new ProducerRecord<String, String>("email_opt_outs","1",
                "navinece_from_java_api@gmail.com");

        producer.send(producer_record);

        for (int key =0; key <=10; key ++) {

            ProducerRecord<String,String> producer_record1 = new ProducerRecord<String, String>("email_opt_outs",Integer.toString(key),
                    "navinece_from_java_api" + Integer.toString(key) + "gmail.com");

            producer.send(producer_record1);
        }


        producer.close();





    }

}

