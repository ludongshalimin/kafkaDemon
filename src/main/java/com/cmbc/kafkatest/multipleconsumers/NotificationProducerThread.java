package com.cmbc.kafkatest.multipleconsumers;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class NotificationProducerThread implements Runnable{
    private final org.apache.kafka.clients.producer.KafkaProducer<String,String> producer;
    private final String topic;
    public NotificationProducerThread(String brokers,String topic){
        Properties props = createProducerConfig(brokers);
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        this.topic = topic;
    }
    private static Properties createProducerConfig(String brokers){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    public void run() {
        System.out.println("producer 3 messages");
//        int i = 0;
//        while(true){
        for(int i = 0;i<1000;i++){
            final String msg = "Message " + i;
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        exception.printStackTrace();
                    }
                    System.out.println("Sent:" + msg + ",Partition: " + metadata.partition() + ", Offset:"+metadata.offset());
                }
            });
            try {
                Thread.sleep(1000);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        //close producer
        producer.close();
    }
}
