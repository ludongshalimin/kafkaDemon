package com.cmbc.kafkatest.singelconsumer;

//单消费者多线程
public class SingleConsumerMain {
    public static void main(String[] args){
        String brokers = "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095";
//        String brokers = "192.168.56.100:9092";
        String groupId = "single-group";
        String topics = "hello_bigdata";
        int numberOfThread = 3;

        if(args != null && args.length >4){
            brokers = args[0];
            groupId = args[1];
            topics = args[2];
            numberOfThread = Integer.parseInt(args[3]);
        }
        //start producer Thread
        NotificationProducerThread producerThread = new NotificationProducerThread(brokers,topics);
        Thread t1 = new Thread(producerThread);
        t1.start();

        //start group of Notification Consumer Thread
        NotificationConsumer consumers = new NotificationConsumer(brokers,groupId,topics);
        consumers.execute(numberOfThread);
        try{
            Thread.sleep(100000);
        }catch(Exception e){
            e.printStackTrace();
        }
        consumers.shutDown();
    }

}
