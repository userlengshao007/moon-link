package com.moon.link.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.enums.MessageType;
import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import com.moon.link.config.KafkaConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PrivateChatKafkaConsumerClient {

    public static void main(String[] args) {
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(buildConsumerProperties());

        consumer.subscribe(Collections.singletonList(KafkaConfig.PRIVATE_CHAT_TOPIC));

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", Integer.getInteger("moon.link.grpc.port", 10000))
                .usePlaintext()
                .build();

        PushServiceGrpc.PushServiceBlockingStub stub =
                PushServiceGrpc.newBlockingStub(channel);

        System.out.println("PrivateChatKafkaConsumerClient started...");
        System.out.println("consume topic: " + KafkaConfig.PRIVATE_CHAT_TOPIC);

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, byte[]> record : records) {
                    handleRecord(record, stub);
                }
            }
        } finally {
            consumer.close();
            channel.shutdown();
        }
    }

    private static Properties buildConsumerProperties() {
        Properties props = new Properties();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConfig.BOOTSTRAP_SERVERS
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                System.getProperty("moon.link.kafka.consumer.group", "moon-im-server-test")
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "true"
        );

        return props;
    }

    private static void handleRecord(ConsumerRecord<String, byte[]> record,
                                     PushServiceGrpc.PushServiceBlockingStub stub) {
        try {
            CompleteMessage message = CompleteMessage.parseFrom(record.value());

            long fromUserId = message.getPacketBody().getFromUserId();
            long toId = message.getPacketBody().getToId();
            String content = message.getPacketBody().getContent();

            System.out.println("consume private chat message:");
            System.out.println("topic = " + record.topic());
            System.out.println("partition = " + record.partition());
            System.out.println("offset = " + record.offset());
            System.out.println("key = " + record.key());
            System.out.println("fromUserId = " + fromUserId);
            System.out.println("toId = " + toId);
            System.out.println("content = " + content);

            PushGrpc.Push2UserRequest request =
                    buildPushRequest(fromUserId, toId, content);

            PushGrpc.Push2UserResponse response =
                    stub.push2User(request);

            System.out.println("push response:");
            System.out.println("success = " + response.getSuccess());
            System.out.println("code = " + response.getCode());
            System.out.println("msg = " + response.getMsg());
            System.out.println("--------------------------------");

        } catch (InvalidProtocolBufferException e) {
            System.out.println("parse kafka message failed: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("handle private chat message failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static PushGrpc.Push2UserRequest buildPushRequest(long fromUserId,
                                                              long toId,
                                                              String content) {
        PushGrpc.PushMessageBody messageBody = PushGrpc.PushMessageBody.newBuilder()
                .setFromUserId(fromUserId)
                .setTimeStamp(System.currentTimeMillis())
                .setMessageType(MessageType.PRIVATE_CHAT_MESSAGE.getType())
                .setContent(content)
                .build();

        return PushGrpc.Push2UserRequest.newBuilder()
                .setToId(toId)
                .setMessage(messageBody)
                .build();
    }
}
