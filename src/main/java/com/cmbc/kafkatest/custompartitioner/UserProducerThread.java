package com.cmbc.kafkatest.custompartitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;

public class UserProducerThread implements Runnable{
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private IUserService userService;

    public UserProducerThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, String>(prop);
        this.topic = topic;
        userService = new UserServiceImpl();
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
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class",
                "com.KafkaUserCustomPartitioner"); //在这个地方放置我们自定义的分区
        return props;
    }
    public void run(){
        System.out.println("Produce Message");
        List<String> users = userService.findAllUsers();
        for(int i = 0;i<1000;i++){
            int pos = (int)(Math.random()*(users.size()));
            final String user = users.get(pos);
            final String msg = "hello" + user;     //根据key来进行分区
            producer.send(new ProducerRecord<String, String>(topic, user, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        exception.printStackTrace();
                    }
                    System.out.println("Send:" +msg + ",User:" + user + ",Partition:" +metadata.partition());
                }
            });
            try{
                Thread.sleep(1000);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
