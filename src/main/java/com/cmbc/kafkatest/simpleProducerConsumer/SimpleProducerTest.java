package com.cmbc.kafkatest.simpleProducerConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * apache kafka simple test
 */
public class SimpleProducerTest {
    public static void main(String[] args){
        //配置参数哪里找,官网了解一下
        //http://kafka.apache.org/090/documentation.html#producerapi
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095"); //ip address of Kafka cluster
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String,String>(props);
            int i = 0;
            while(true){
                String msg = "Message " + i;
                producer.send(new ProducerRecord<String, String>("hello_bigdata", msg));
                System.out.println("Sent:" + msg);
                try{
                    Thread.sleep(2000);
                } catch (Exception e){
                    e.printStackTrace();
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }

    }

}
