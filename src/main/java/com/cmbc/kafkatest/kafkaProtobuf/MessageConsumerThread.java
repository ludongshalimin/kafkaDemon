package com.cmbc.kafkatest.kafkaProtobuf;


import com.proto.protobuftest.PersonEntity.Person;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MessageConsumerThread implements Runnable {
    private final KafkaConsumer<Integer,Person> consumer;
    private final String topic;
    public MessageConsumerThread(String brokers,String groupId,String topic) {
        Properties prop = createConsumerConfig(brokers,groupId);
//        this.consumer = new KafkaConsumer<Integer, Person>(prop,new IntegerDeserializer(), new MessageDeserializer());
        this.consumer = new KafkaConsumer<Integer, Person>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }
    private static Properties createConsumerConfig(String brokers,String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "com.MessageDeserializer");  //配置反序列化
        return props;
    }
    public void run() {
        while(true) {
            ConsumerRecords<Integer,Person> records = consumer.poll(1000);
            for(final ConsumerRecord<Integer,Person> record:records) {
                System.out.println("Receiver key:" + record.key()+"receiver value:" +record.value() + "offset:" + record.offset());
            }
        }
    }
}
