package com.cmbc.kafkatest.kafkaProtobuf;

import com.google.protobuf.InvalidProtocolBufferException;

import com.proto.protobuftest.PersonEntity.Person;

import org.apache.kafka.common.serialization.Deserializer;


public class MessageDeserializer extends Adapter implements Deserializer<Person> {
    public Person deserialize(final String topic, final byte[] data) {
        try {
            return Person.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
        }
    }
}
