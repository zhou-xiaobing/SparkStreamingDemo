import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;


/**
 * @Description
 * @Author Zhou-Xiaobing
 * @Date 2019/9/18 9:59
 */
public class JunitTest {
    @Test
    public void testPrint() {
        System.out.println("hello,world");
    }

    @Test
    public void testKafkaProductor() {
        String topic = "my_topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "ambari-1:6667");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>(topic, "this is my " + i + " message"));

        producer.close();
    }

    @Test
    public void testKafkaConsumer() {
        String topic = "my_topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "ambari-1:6667");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
    @Test
    public void testLoop(){
        for (int i = 0; i < 100; i++) {
            String info = "**"+i;
            System.out.println(info);
        }
    }
}
