package com.cmbc.kafkatest.customserializer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UserProducerThread implements Runnable{
    private final KafkaProducer<String,User>producer;
    private final String topic;
    public UserProducerThread(String brokers,String topic){
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, User>(prop);
        this.topic = topic;
    }
    private static Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.UserSerializer");  //在这个地方配置序列化类

        return props;
    }
    public void run() {
        List<User> users = new ArrayList();
        users.add(new User( 1L,"tom","Tom","Riddle",40));
        users.add(new User(2L,"bob","BOB","James",50));

        for(final User user:users){
            producer.send(new ProducerRecord<String, User>(topic,user.getUserName(),user),new Callback(){
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                     if( exception != null){
                         exception.printStackTrace();
                     }
                     System.out.println("sent:" + user.toString());
                }
            });
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
