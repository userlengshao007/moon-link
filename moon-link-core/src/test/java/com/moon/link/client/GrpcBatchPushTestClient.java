package com.moon.link.client;

import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcBatchPushTestClient {

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 10000)
                .usePlaintext()
                .build();

        PushServiceGrpc.PushServiceBlockingStub stub =
                PushServiceGrpc.newBlockingStub(channel);

        PushGrpc.PushMessageBody message = PushGrpc.PushMessageBody.newBuilder()
                .setFromUserId(20001)
                .setTimeStamp(System.currentTimeMillis())
                .setMessageType(4)
                .setContent("hello, this is a grpc batch push message")
                .build();

        PushGrpc.Push2UsersRequest request = PushGrpc.Push2UsersRequest.newBuilder()
                .addToIds(10001)
                .addToIds(10002)
                .addToIds(10003)
                .setMessage(message)
                .build();

        PushGrpc.Push2UsersResponse response = stub.push2Users(request);

        System.out.println("total = " + response.getTotal());
        System.out.println("successCount = " + response.getSuccessCount());
        System.out.println("failCount = " + response.getFailCount());

        for (PushGrpc.PushResult result : response.getResultsList()) {
            System.out.println(
                    "toId = " + result.getToId()
                            + ", success = " + result.getSuccess()
                            + ", code = " + result.getCode()
                            + ", msg = " + result.getMsg()
            );
        }

        channel.shutdown();
    }
}
