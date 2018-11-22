package com.cmbc.kafkatest.singelconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerThreadHandler implements Runnable{
    private ConsumerRecord consumerRecord;
    public ConsumerThreadHandler(ConsumerRecord consumerRecord){
        this.consumerRecord = consumerRecord;
    }
    public void run(){
        //数据处理逻辑
        System.out.println("Receive message: " + consumerRecord.value() + ", Partition: " +
                consumerRecord.partition() + " Offset: " + consumerRecord.offset()
                + ", By ThreadID: " + Thread.currentThread().getId());
    }
}
