package com.cmbc.kafkatest.singelconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NotificationConsumer {
    private final KafkaConsumer<String,String>consumer;
    private final String topic;
    //Threadpool of consumers
    private ExecutorService executor;
    public NotificationConsumer(String brokers,String groupId,String topic){
        Properties prop = createConsumerConfig(brokers,groupId);
        this.consumer = new KafkaConsumer<String,String>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }
    public void execute(int numberOfThreads){    //使用线程池来管理
        executor = new ThreadPoolExecutor(numberOfThreads,numberOfThreads,0L, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(1000),new ThreadPoolExecutor.CallerRunsPolicy());
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(final ConsumerRecord record :records){
                executor.submit(new ConsumerThreadHandler(record));
            }
        }
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
    public void shutDown(){
        if(consumer != null){
            consumer.close();
        }
        if(executor != null){
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out
                        .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
}
