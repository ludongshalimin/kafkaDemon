package com.cmbc.kafkatest.custompartitioner;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class UserConsumerThread implements Runnable{
    private final KafkaConsumer<String,String> consumer;
    private final String topic;
    public UserConsumerThread(String brokers,String groupId,String topic){
        Properties prop = createConsumerConfig(brokers,groupId);
        this.consumer = new KafkaConsumer<String, String>(prop);
        this.topic = topic;
        /*
        subscribe指定消费者模式为：topic,由kafka指定分配策略
        assign指定消费模式为指定partition,由用户自定义partition分配策略
         */
        this.consumer.subscribe(Arrays.asList(this.topic));
        //this.consumer.assign(Arrays.asList(new TopicPartition(this.topic,0)));

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
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    public void run() {
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(final ConsumerRecord record:records){
                System.out.println("Receiver message:" +record.value() + ", Partition:"+
                record.partition() + ",Offset:" + record.offset());
            }
        }
    }
}
