package com.moon.link.codec;

import com.moon.link.common.constant.ProtoConstant;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.utils.ProtoUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

import static com.moon.link.common.constant.ProtoConstant.DEFAULT_SECRETKEY;

@Slf4j
public class MessageProtocolEncoder extends MessageToByteEncoder<CompleteMessage> {
    /**
     * 将完整的消息转换成为字节流
     *
     * @param channelHandlerContext 上下文环境
     * @param message 要编码的完整信息
     * @param out 将二进制数据写入这里
     */
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, CompleteMessage message, ByteBuf out) throws Exception {
        log.info("[Outbound-encodeData] uid: {}, messageType: {}",
                message.getPacketHeader().getUid(), message.getPacketHeader().getMessageType());
        // 依次序列化
        // 包边界
        out.writeShort(ProtoConstant.MAGIC);
        out.writeShort(ProtoConstant.VERSION);

        // 从完整message中拆出 包头和包体
        PacketHeader packetHeader = message.getPacketHeader();
        PacketBody messageBody = message.getPacketBody();

        // header data: protobuf对象 -> 二进制数组
        byte[] headerBytes = packetHeader.toByteArray();

        // body data: protobuf对象 -> 二进制数组
        byte[] dataBytes = messageBody.toByteArray();

        // 序列化包头长度
        out.writeInt(headerBytes.length);

        // 判断是否压缩和加密，先压缩，再加密
        byte compression = (byte) packetHeader.getCompression();
        if (compression == 1) {
            // 使用 gzip 压缩
            dataBytes = ProtoUtil.compress(dataBytes);
        }
        byte encryption = (byte) packetHeader.getEncryption();
        if (encryption == 1) {
            // 使用 aes 加密
            dataBytes = ProtoUtil.encryptAES(dataBytes, DEFAULT_SECRETKEY.getBytes(StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8);
        }

        // 序列化包体长度
        out.writeInt(dataBytes.length);
        log.info("[HandledDataLength] 压缩+加密完 包体长度: " + dataBytes.length);

        // 序列化包头
        out.writeBytes(headerBytes);
        // 序列化包体
        out.writeBytes(dataBytes);
    }
}
