package com.cmbc.kafkatest.kafkaProtobuf;

import com.proto.protobuftest.PersonEntity.Person;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageProducerThread implements Runnable{
    private final KafkaProducer<Integer,Person> producer;
    private final String topic;
    public MessageProducerThread(String brokers,String topic) {
        Properties prop = createProducerConfig(brokers);
//        this.producer = new KafkaProducer<Integer,Person>(prop,new IntegerSerializer(),new MessageSerializer());
        this.producer = new KafkaProducer<Integer, Person>(prop);
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
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "com.MessageSerializer");
//        props.put("partitioner.class", "com.KafkaUserCustomPartitioner"); //在这个地方放置我们自定义的分区
        return props;
    }
    public void run() {
        System.out.println("Produce Message");
        for(int i = 0;i<1000;i++) {
//            Person sasa =
//                    Person.newBuilder()
//                            .setId(1234)
//                            .setName("John")
//                            .setEmail("jdoe@example.com")
//                            .addPhone(
//                                    Person.PhoneNumber.newBuilder()
//                                            .setNumber("555-4321")
//                                            .setType(Person.PhoneType.HOME))
//                            .build();
            Person sasa = Person.newBuilder().setId(15)
                    .setEmail("ludongshalimin@163.com")
                    .setName("shalimin")
                    .build();
            try {

                producer.send(new ProducerRecord<Integer, Person>(topic,i,sasa)).get();
                System.out.println("send message :" + sasa.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            try{
                Thread.sleep(1000);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
