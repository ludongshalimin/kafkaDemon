package com.cmbc.kafkatest.multipleconsumers;

/**
 * 多线程kafka消费者
 * 为什么要用多线程kakfa消费者呢？
 * 如果生产者生产的数据增长的过快
 * 虽然每一条还没有被处理的消息仍然在kafka中，但是当消息的数量巨大的时候，就很危险了
 * 有些数据可能到达我们的设定的“保留策略”retention policy,存在数据丢失的可能性
 *
 * 目前kafka 的保留策略(time-based,partition-based,key-based)
 *
 * 多线程kafka消费模型两种：
 * 1：单线程多消费者（Multiple consumers with their own threads）
 * 2：单消费者多处理线程(Single consumer,multiple worker processing threads)
 *
 * model1:优点：
 *          1：容易实现
 *          2：在每个分区上实现按顺序处理更容易
 *         缺点：
 *          1：消费者的数量受限于topic的分区数量。多于消费者分区的消费者只能等待
 *          2：对于brokers来说更多的TCP 连接
 * model2:优点
 *          可以灵活拓展处理线程的数量
 *        缺点：
 *          在每个分区上实现按顺序处理并不容易。假设在2个不同线程处理的同一分区上有2条消息。
 *          为了保证订单，必须以某种方式协调这两个线程
 *
 */
//多消费者，单线程
public class MultipleConsumersMain {
    public static void main(String[] args){
        String brokers = "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095";
        String groupId = "multiple-group";
        String topic = "hello_bigdata";

        int numberOfConsumer = 3;

        if(args != null && args.length > 4){
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            numberOfConsumer = Integer.parseInt(args[3]);
        }
        //start Notification Producer Thread
        NotificationProducerThread producerThread = new NotificationProducerThread(brokers,topic);
        Thread t1 = new Thread(producerThread);
        t1.start();

        //start group fo Notification Consumers
        NotificationConsumerGroup consumerGroup = new NotificationConsumerGroup(brokers,groupId,topic,numberOfConsumer);

        consumerGroup.execute();

        try{
            Thread.sleep(10000);
        }catch(InterruptedException ie){
            ie.printStackTrace();
        }

    }
}
