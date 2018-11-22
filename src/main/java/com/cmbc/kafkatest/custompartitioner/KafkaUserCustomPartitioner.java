package com.cmbc.kafkatest.custompartitioner;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class KafkaUserCustomPartitioner implements Partitioner {
    private IUserService userService;
    public KafkaUserCustomPartitioner(){
        userService = new UserServiceImpl();
    }
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
       int partition = 0;
       String userName = (String) key;
       //find the id of current user based on the username
        Integer userId = userService.findUserId(userName);
        //if the userId not found ,default partition is 0
        if(userId != null){
            partition = userId;
        }
        return partition;
    }

    public void configure(Map<String, ?> configs) {

    }

    public void close() {

    }
}
