package com.cmbc.kafkatest.customserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class UserDeserializer implements Deserializer<User>{
    public User deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        User user = null;
        try{
            user = mapper.readValue(data,User.class);
        }catch(Exception e){
            e.printStackTrace();
        }
        return user;
    }

    public void close() {

    }

    public void configure(Map configs, boolean isKey) {

    }
}
