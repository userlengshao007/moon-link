package com.moon.link.client;

import com.moon.link.common.grpc.PushGrpc;
import com.moon.link.common.grpc.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcPushTestClient {
    /**
     * gRPC推送测试客户端入口
     * 
     * 模拟客户端通过gRPC调用推送服务，向指定用户发送消息。
     * 测试流程：建立连接 -> 构建请求 -> 调用服务 -> 输出结果 -> 关闭连接
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        // 创建gRPC通道，连接到本地10000端口的服务端
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", Integer.getInteger("moon.link.grpc.port", 10000))
                .usePlaintext()
                .build();

        // 创建阻塞式gRPC存根用于同步调用
        PushServiceGrpc.PushServiceBlockingStub stub =
                PushServiceGrpc.newBlockingStub(channel);

        // 构建推送消息体，包含发送者、时间戳、类型和内容
        PushGrpc.PushMessageBody message = PushGrpc.PushMessageBody.newBuilder()
                .setFromUserId(20001)
                .setTimeStamp(System.currentTimeMillis())
                .setMessageType(4)
                .setContent("hello, this is a grpc push message")
                .build();

        // 构建推送请求，指定目标用户ID和消息内容
        PushGrpc.Push2UserRequest request = PushGrpc.Push2UserRequest.newBuilder()
                .setToId(10001)
                .setMessage(message)
                .build();

        // 调用gRPC推送服务
        PushGrpc.Push2UserResponse response = stub.push2User(request);

        // 输出响应结果
        System.out.println("success = " + response.getSuccess());
        System.out.println("code = " + response.getCode());
        System.out.println("msg = " + response.getMsg());

        // 关闭gRPC通道
        channel.shutdown();
    }
}
