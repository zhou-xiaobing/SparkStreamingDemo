package com.angelreef.KafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Description
 * @Author Zhou-Xiaobing
 * @Date 2019/9/11 11:32
 */
public class ConsumerDemo extends Thread {
    private String topic = "my_topic";
    KafkaConsumer<String, String> consumer;

    public ConsumerDemo() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ambari-1:6667");
        properties.setProperty("group.id","test");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> recode: records) {
                System.out.println("recodeOffset = " + recode.offset() + "recodeValue = this is " + recode.value());
            }
        }
    }
    public static void main(String[] args){
        new ConsumerDemo().start();
    }

}
