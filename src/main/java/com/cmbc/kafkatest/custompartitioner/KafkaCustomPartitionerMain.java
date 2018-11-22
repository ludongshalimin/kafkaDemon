package com.cmbc.kafkatest.custompartitioner;

/**
 *  自定义分区有什么好处？
 *  默认情况下，producer通过循环的方式向不同的分区发送消息
 *  producer 发送消息到broker时，会根据partion机制选择将其存储到哪一个partition,
 *  如果partition 机制设置合理，所有消息可以均匀分布到不同的partition里面，这样就实现了负载均衡。
 *  如果一个topic对应一个文件，那这个文件所在的机器IO将会成为这个TOPIC的性能瓶颈
 *  而有了partition后，不同消息可以并行写入不同 broker的不同partition里面，极大地提高了吞吐率
 */
public class KafkaCustomPartitionerMain {
    public static void main(String[] args){
        String brokers = "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095";
        String groupId = "partition-group";
        String topic = "hello_bigdata";

        if(args != null && args.length ==3){
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
        }
        //start User Producer Thread
        UserProducerThread producerThread =  new UserProducerThread(brokers,topic);
        Thread t1 = new Thread(producerThread);
        t1.start();
        //start group of User Consumer Thread
        UserConsumerThread consumerThread = new UserConsumerThread(brokers,groupId,topic);
        Thread t2 = new Thread(consumerThread);
        t2.start();
        try {
            Thread.sleep(10000);
        }catch(InterruptedException ie){
            ie.printStackTrace();
        }
    }
}
