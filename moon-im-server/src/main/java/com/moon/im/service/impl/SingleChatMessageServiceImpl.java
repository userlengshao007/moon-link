package com.moon.im.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.moon.im.constant.MessagePushStatus;
import com.moon.im.domain.SingleChatMessage;
import com.moon.im.mapper.SingleChatMessageMapper;
import com.moon.im.service.SingleChatMessageService;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class SingleChatMessageServiceImpl
        extends ServiceImpl<SingleChatMessageMapper, SingleChatMessage>
        implements SingleChatMessageService {

    private static final int MAX_RANGE_SIZE = 200;
    private static final int DEFAULT_HISTORY_LIMIT = 20;
    private static final int MAX_HISTORY_LIMIT = 100;
    private static final int MAX_UNPUSHED_LIMIT = 100;

    @Override
    public List<SingleChatMessage> listBySeqRange(String cid, long startSeqId, long endSeqId) {
        checkCid(cid);
        if (startSeqId <= 0 || endSeqId <= 0 || startSeqId > endSeqId) {
            throw new IllegalArgumentException("invalid seq range");
        }
        if (endSeqId - startSeqId + 1 > MAX_RANGE_SIZE) {
            throw new IllegalArgumentException("seq range too large");
        }

        return list(new LambdaQueryWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getCid, cid)
                .between(SingleChatMessage::getSeqId, startSeqId, endSeqId)
                .orderByAsc(SingleChatMessage::getSeqId));
    }

    @Override
    public List<SingleChatMessage> listHistory(String cid, Long beforeSeqId, int limit) {
        checkCid(cid);
        int safeLimit = normalizeLimit(limit);

        LambdaQueryWrapper<SingleChatMessage> wrapper = new LambdaQueryWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getCid, cid)
                .orderByDesc(SingleChatMessage::getSeqId)
                .last("LIMIT " + safeLimit);

        if (beforeSeqId != null && beforeSeqId > 0) {
            wrapper.lt(SingleChatMessage::getSeqId, beforeSeqId);
        }

        List<SingleChatMessage> messages = list(wrapper);
        Collections.reverse(messages);
        return messages;
    }

    @Override
    public List<SingleChatMessage> listUnpushedMessages(long toUserId, int limit) {
        int safeLimit = limit <= 0 ? MAX_UNPUSHED_LIMIT : Math.min(limit, MAX_UNPUSHED_LIMIT);

        return list(new LambdaQueryWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getToUserId, toUserId)
                .eq(SingleChatMessage::getStatus, MessagePushStatus.PUSH_FAILED)
                .orderByAsc(SingleChatMessage::getCreateTime)
                .last("LIMIT " + safeLimit));
    }

    @Override
    public void updatePushStatus(Long id, int status) {
        update(new LambdaUpdateWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getId, id)
                .set(SingleChatMessage::getStatus, status)
                .set(SingleChatMessage::getUpdateTime, java.time.LocalDateTime.now()));
    }

    private void checkCid(String cid) {
        if (cid == null || cid.isBlank()) {
            throw new IllegalArgumentException("cid must not be blank");
        }
    }

    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_HISTORY_LIMIT;
        }
        return Math.min(limit, MAX_HISTORY_LIMIT);
    }
}
