package com.cmbc.kafkatest.customserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserSerializer implements Serializer<User>{
    public void close() {

    }

    public byte[] serialize(String topic, User data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            retVal = objectMapper.writeValueAsString(data).getBytes();
        }catch(Exception e){
            e.printStackTrace();
        }
        return retVal;
    }

    public void configure(Map configs, boolean isKey) {

    }
}
