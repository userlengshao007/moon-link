package com.moon.link.codec;

import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 消息协议编解码器测试类
 * 测试编码器和解码器的正确性、半包粘包处理、压缩加密功能
 */
public class MessageProtocolCodecTest {

    /**
     * 测试:编码后再解码,消息字段应该保持不变
     * 验证基本的编解码流程
     */
    @Test
    public void encodeThenDecodeShouldKeepMessageFields() {
        // 构建一个不压缩不加密的测试消息
        CompleteMessage message = buildMessage(false, false);
        
        // 创建嵌入式的编码通道和解码通道(用于测试,无需真实网络)
        EmbeddedChannel encoderChannel = new EmbeddedChannel(new MessageProtocolEncoder());
        EmbeddedChannel decoderChannel = new EmbeddedChannel(new MessageProtocolDecoder());

        // 编码器:将消息写入出站通道
        assertTrue(encoderChannel.writeOutbound(message));
        ByteBuf encoded = encoderChannel.readOutbound();
        assertNotNull(encoded);
        log(encoded);

        // 解码器:将编码后的数据写入入站通道
        assertTrue(decoderChannel.writeInbound(encoded));
        CompleteMessage decoded = decoderChannel.readInbound();

        // 验证解码后的消息与原始消息一致
        assertNotNull(decoded);
        assertEquals(message.getPacketHeader(), decoded.getPacketHeader());
        assertEquals(message.getPacketBody(), decoded.getPacketBody());
        
        // 关闭通道,确认没有残留数据
        assertFalse(encoderChannel.finish());
        assertFalse(decoderChannel.finish());
    }

    /**
     * 测试:解码器应该等待完整的数据帧
     * 验证半包场景下,解码器不会错误解析不完整的数据
     */
    @Test
    public void decoderShouldWaitForCompleteFrame() {
        CompleteMessage message = buildMessage(false, false);
        EmbeddedChannel encoderChannel = new EmbeddedChannel(new MessageProtocolEncoder());
        EmbeddedChannel decoderChannel = new EmbeddedChannel(new MessageProtocolDecoder());

        // 先编码完整的消息
        assertTrue(encoderChannel.writeOutbound(message));
        ByteBuf encoded = encoderChannel.readOutbound();
        assertNotNull(encoded);

        // 将编码后的数据拆分成两部分:前8字节和剩余部分
        ByteBuf firstPart = encoded.readRetainedSlice(8);  // 只包含魔数、版本、包头长度
        ByteBuf secondPart = encoded.readRetainedSlice(encoded.readableBytes());  // 剩余所有数据
        encoded.release();

        // 写入第一部分(不完整的数据),解码器应该不输出任何消息
        assertFalse(decoderChannel.writeInbound(firstPart));
        assertNull(decoderChannel.readInbound());  // 验证没有解码出消息

        // 写入第二部分(完整数据),解码器应该成功解码
        assertTrue(decoderChannel.writeInbound(secondPart));
        CompleteMessage decoded = decoderChannel.readInbound();

        // 验证解码后的消息字段正确
        assertNotNull(decoded);
        assertEquals(message.getPacketHeader().getUid(), decoded.getPacketHeader().getUid());
        assertEquals(message.getPacketBody().getContent(), decoded.getPacketBody().getContent());
        
        assertFalse(encoderChannel.finish());
        assertFalse(decoderChannel.finish());
    }

    /**
     * 测试:编码解码应该支持压缩和加密
     * 验证数据经过压缩+加密后再解密+解压缩,内容保持一致
     */
    @Test
    public void encodeThenDecodeShouldSupportCompressionAndEncryption() {
        // 构建一个开启压缩和加密的测试消息
        CompleteMessage message = buildMessage(true, true);
        EmbeddedChannel encoderChannel = new EmbeddedChannel(new MessageProtocolEncoder());
        EmbeddedChannel decoderChannel = new EmbeddedChannel(new MessageProtocolDecoder());

        // 编码:消息 → 压缩 → 加密 → 二进制
        assertTrue(encoderChannel.writeOutbound(message));
        ByteBuf encoded = encoderChannel.readOutbound();
        assertNotNull(encoded);

        // 解码:二进制 → 解密 → 解压缩 → 消息
        assertTrue(decoderChannel.writeInbound(encoded));
        CompleteMessage decoded = decoderChannel.readInbound();

        // 验证压缩加密后的数据能正确还原
        assertNotNull(decoded);
        assertEquals(message.getPacketHeader(), decoded.getPacketHeader());
        assertEquals(message.getPacketBody(), decoded.getPacketBody());
        
        assertFalse(encoderChannel.finish());
        assertFalse(decoderChannel.finish());
    }

    /**
     * 构建测试消息
     * @param compression 是否开启压缩
     * @param encryption 是否开启加密
     * @return 完整的消息对象
     */
    private CompleteMessage buildMessage(boolean compression, boolean encryption) {
        // 构建消息头
        PacketHeader header = PacketHeader.newBuilder()
                .setAppId(1)                    // 业务线ID
                .setUid(10001L)                 // 用户ID
                .setToken("test-token")         // 登录凭证
                .setCompression(compression ? 1 : 0)  // 压缩标志
                .setEncryption(encryption ? 1 : 0)    // 加密标志
                .setMessageType(1)              // 消息类型:1-心跳
                .build();

        // 构建消息体
        PacketBody body = PacketBody.newBuilder()
                .setFromUserId(10001L)          // 发送方用户ID
                .setToId(10002L)                // 接收方ID
                .setTimeStamp(1780190180910L)   // 时间戳
                .setMessageType(4)              // 消息类型:4-私聊
                .setContent("hello moon-link")  // 消息内容
                .build();

        // 组合成完整消息
        return CompleteMessage.newBuilder()
                .setPacketHeader(header)
                .setPacketBody(body)
                .build();
    }

    public static void log(ByteBuf buffer) {
        int length = buffer.readableBytes();
        int rows = length / 16 + (length % 15 == 0 ? 0 : 1) + 4;
        StringBuilder buf = new StringBuilder(rows * 80 * 2)
                .append("read index:").append(buffer.readerIndex())
                .append(" write index:").append(buffer.writerIndex())
                .append(" capacity:").append(buffer.capacity())
                .append(NEWLINE);
        appendPrettyHexDump(buf, buffer);
        System.out.println(buf.toString());
    }
}
