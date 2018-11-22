package com.cmbc.kafkatest.kafkaProtobuf;

import com.proto.protobuftest.PersonEntity.Person;

import org.apache.kafka.common.serialization.Serializer;


public class MessageSerializer extends Adapter implements Serializer<Person>{

    public byte[] serialize(final String topic, final Person data) {
        return data.toByteArray();
    }
}
