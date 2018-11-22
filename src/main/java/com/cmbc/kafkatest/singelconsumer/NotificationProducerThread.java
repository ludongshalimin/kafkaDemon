package com.cmbc.kafkatest.singelconsumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class NotificationProducerThread implements Runnable{
    private final KafkaProducer<String,String> producer;
    private final String topic;

    public NotificationProducerThread(String brokers,String topic){
        Properties props = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String,String>(props);
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
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void run() {
        System.out.println("Produce 5 message");
        for(int i = 0;i<1000;i++){
            final String msg = "Message " + i;
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        exception.printStackTrace();
                    }
                    System.out.println("Sent:" + msg + ",offset:" +metadata.offset());
                }
            });
            try{
                Thread.sleep(1000);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        //close producer
        producer.close();
    }
}
