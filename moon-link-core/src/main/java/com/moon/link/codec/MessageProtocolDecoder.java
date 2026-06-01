package com.moon.link.codec;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.utils.ProtoUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;


import static com.moon.link.common.constant.ProtoConstant.*;


@Slf4j
public class MessageProtocolDecoder extends ByteToMessageDecoder {
    /**
     * 解码
     *
     * @param ctx 上下文环境
     * @param in 输入缓冲区
     * @param out 输出列表
     * @throws Exception
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 长度不足包边界，返回，等待更多数据的到达
        if (in.readableBytes() < FIXED_HEADER_LENGTH) {
            return;
        }

        // 标记当前读取指针位置，便于后续重置
        in.markReaderIndex();

        // 读取固定字段
        short magic = in.readShort();
        short version = in.readShort();
        int headerLength = in.readInt();
        int dataLength = in.readInt();

        if (magic != MAGIC) {
            ctx.close();
            return;
        }

        if (version != VERSION) {
            ctx.close();
            return;
        }

        if (headerLength < 0 || dataLength < 0) {
            ctx.close();
            return;
        }

        long frameLength = (long) headerLength + dataLength;
        if (frameLength > MAX_FRAME_LENGTH) {
            ctx.close();
            return;
        }

        // 数据不足包头和包体，返回
        if (in.readableBytes() < frameLength) {
            // 重置读指针
            in.resetReaderIndex();
            return;
        }

        // 解析包头和包体，构造 byte数组
        byte[] headerBytes = new byte[headerLength];
        byte[] dataBytes = new byte[dataLength];
        // 缓冲区读到 byte 数组里，方便Protobuf的解析
        in.readBytes(headerBytes);
        in.readBytes(dataBytes);

        // header data: 
        PacketHeader packetHeader = PacketHeader.parseFrom(headerBytes);

        log.info("[DecodeMessageProtocol] uid: {}, messageType: {}",
                packetHeader.getUid(), packetHeader.getMessageType());

        // 对 dataBytes 先解密
        if (packetHeader.getEncryption() == 1) {
            // bytes 先转string，解密完转 bytes
            dataBytes = ProtoUtil.decryptAES(new String(dataBytes, StandardCharsets.UTF_8),
                    DEFAULT_SECRETKEY.getBytes(StandardCharsets.UTF_8));
        }
        // 解压缩
        if (packetHeader.getCompression() == 1) {
            dataBytes = ProtoUtil.decompress(dataBytes);
        }

        // body data: 二进制数组 -> protobuf对象
        PacketBody packetBody = PacketBody.parseFrom(dataBytes);

        out.add(CompleteMessage.newBuilder().setPacketHeader(packetHeader).setPacketBody(packetBody).build());
    }
}
