package com.moon.link.handler;

import com.moon.link.cache.UserChannelCtxMap;
import com.moon.link.common.domain.protobuf.CompleteMessage;
import com.moon.link.common.domain.protobuf.PacketBody;
import com.moon.link.common.domain.protobuf.PacketHeader;
import com.moon.link.common.enums.MessageType;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinkChannelHandlerTest {

    @Test
    public void loginThenCloseShouldRemoveUserChannelMapping() {
        long uid = 10001L;
        UserChannelCtxMap.remove(uid);
        EmbeddedChannel channel = new EmbeddedChannel(new LinkChannelHandler());

        channel.writeInbound(buildLoginMessage(uid));
        assertTrue(UserChannelCtxMap.contains(uid));

        channel.close();

        assertFalse(UserChannelCtxMap.contains(uid));
    }

    private CompleteMessage buildLoginMessage(long uid) {
        return CompleteMessage.newBuilder()
                .setPacketHeader(PacketHeader.newBuilder()
                        .setAppId(1)
                        .setUid(uid)
                        .setToken("test-token")
                        .setMessageType(MessageType.LOGIN_MESSAGE.getType())
                        .build())
                .setPacketBody(PacketBody.newBuilder()
                        .setFromUserId(uid)
                        .setTimeStamp(System.currentTimeMillis())
                        .setMessageType(MessageType.LOGIN_MESSAGE.getType())
                        .setContent("login request")
                        .build())
                .build();
    }
}
