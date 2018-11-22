package com.cmbc.kafkatest.kafkaProtobuf;


public class KafkaProtobufMain {
    public static void main(String[] args) {
        String brokers = "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095";
        String groupId = "proto-group";
        String topic = "hello_person";  //这是我新建立的一个topic用于测试kafka protobuf序列化传输

        if (args != null &&args.length == 3){
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
        }
        //start User Producer Thread
        MessageProducerThread producerThread = new MessageProducerThread(brokers,topic);
        Thread t1 = new Thread(producerThread);
        t1.start();

        //start group of user consumer Thread
        MessageConsumerThread consumerThread = new MessageConsumerThread(brokers,groupId,topic);
        Thread t2 = new Thread(consumerThread);
        t2.start();
        try{
            Thread.sleep(1000000);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
