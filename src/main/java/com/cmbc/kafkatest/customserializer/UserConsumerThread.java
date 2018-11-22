package com.cmbc.kafkatest.customserializer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class UserConsumerThread implements Runnable {
    private final KafkaConsumer<String,User> consumer;
    private final String topic;
    public UserConsumerThread(String brokers,String groupId,String topic){
        Properties prop = createConsumerConfig(brokers,groupId);
        this.consumer = new KafkaConsumer<String, User>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }
    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.UserDeserializer");  //配置反序列化
        return props;
    }
    public void run() {
        while(true){
            ConsumerRecords<String,User> records = consumer.poll(100);
            for(final ConsumerRecord<String,User> record:records){
                System.out.println("Receiver:" + record.value().toString());
            }
        }
    }
}
