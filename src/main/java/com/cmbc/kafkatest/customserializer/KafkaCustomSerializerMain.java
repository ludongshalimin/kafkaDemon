package com.cmbc.kafkatest.customserializer;

/**
 * kafka序列化
 * 为什么需要序列化
 * kafka允许我们用不同的数据类型向broker发送数据，类型可以是字符串，数字，数组甚至是其他任何类型的对象
 * kafka支持下面的序列化和反序列化：ByteArray,Integer,Long,String
 * 我们一般选择序列化库比如：Avro,Thrift,Protocol buffer
 *
 */
public class KafkaCustomSerializerMain {
    public static void main(String[] args) {
        String brokers = "192.168.56.100:9093,192.168.56.100:9094,192.168.56.100:9095";
        String groupId = "group01";
        String topic = "hello_bigdata";

        if (args != null &&args.length == 3){
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
        }
        //start User Producer Thread
        UserProducerThread producerThread = new UserProducerThread(brokers,topic);
        Thread t1 = new Thread(producerThread);
        t1.start();

        //start group of user consumer Thread
        UserConsumerThread consumerThread = new UserConsumerThread(brokers,groupId,topic);
        Thread t2 = new Thread(consumerThread);
        t2.start();
        try{
            Thread.sleep(10000);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
