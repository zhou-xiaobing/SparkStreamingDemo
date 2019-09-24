package com.angelreef.KafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description
 * @Author Zhou-Xiaobing
 * @Date 2019/9/11 11:31
 */

public class ProducerDemo extends Thread {
    private String topic;
    private KafkaProducer<String,String> producer;

    public ProducerDemo(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("bootstrap.servers","ambari-1:6667");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true){
            String message = "message" + messageNo;
            System.out.println("send = "+message);
            producer.send(new ProducerRecord<String, String>(topic,message));
            messageNo ++;

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        new ProducerDemo("my_topic").start();
    }
}