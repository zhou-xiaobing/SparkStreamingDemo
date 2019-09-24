package com.angelreef.KafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description
 * @Author Zhou-Xiaobing
 * @Date 2019/9/19 9:45
 */
public class myProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ambari-1:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 1000; i < 2000; i++) {
            String message = new String("this is message" + i);
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>("message", message));
        }
//        producer.close();
//        int num = 0;
//        while(true){
//            String message = new String("message" + num);
//            producer.send(new ProducerRecord<String, String>("message", message));
//            System.out.println("send = "+message);
//            num++;
//        }
    }
}
