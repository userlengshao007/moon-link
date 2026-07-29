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

/**
 * 单聊消息服务实现，负责限制查询窗口并构造持久层查询条件。
 */
@Service
public class SingleChatMessageServiceImpl
        extends ServiceImpl<SingleChatMessageMapper, SingleChatMessage>
        implements SingleChatMessageService {

    private static final int MAX_RANGE_SIZE = 200;
    private static final int DEFAULT_HISTORY_LIMIT = 20;
    private static final int MAX_HISTORY_LIMIT = 100;
    private static final int MAX_UNPUSHED_LIMIT = 100;

    /** {@inheritDoc} */
    @Override
    public List<SingleChatMessage> listBySeqRange(String cid, long startSeqId, long endSeqId) {
        checkCid(cid);
        if (startSeqId <= 0 || endSeqId <= 0 || startSeqId > endSeqId) {
            throw new IllegalArgumentException("invalid seq range");
        }
        // 限制单次序号跨度，避免客户端构造超大范围查询拖慢数据库。
        if (endSeqId - startSeqId + 1 > MAX_RANGE_SIZE) {
            throw new IllegalArgumentException("seq range too large");
        }

        return list(new LambdaQueryWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getCid, cid)
                .between(SingleChatMessage::getSeqId, startSeqId, endSeqId)
                .orderByAsc(SingleChatMessage::getSeqId));
    }

    /** {@inheritDoc} */
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
        // 数据库倒序取最近一页，返回前恢复为聊天界面需要的正序。
        Collections.reverse(messages);
        return messages;
    }

    /** {@inheritDoc} */
    @Override
    public List<SingleChatMessage> listUnpushedMessages(long toUserId, int limit) {
        int safeLimit = limit <= 0 ? MAX_UNPUSHED_LIMIT : Math.min(limit, MAX_UNPUSHED_LIMIT);

        return list(new LambdaQueryWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getToUserId, toUserId)
                .eq(SingleChatMessage::getStatus, MessagePushStatus.PUSH_FAILED)
                .orderByAsc(SingleChatMessage::getCreateTime)
                .last("LIMIT " + safeLimit));
    }

    /** {@inheritDoc} */
    @Override
    public void updatePushStatus(Long id, int status) {
        update(new LambdaUpdateWrapper<SingleChatMessage>()
                .eq(SingleChatMessage::getId, id)
                .set(SingleChatMessage::getStatus, status)
                .set(SingleChatMessage::getUpdateTime, java.time.LocalDateTime.now()));
    }

    /**
     * 校验会话 ID 是否可用于查询。
     *
     * @param cid 会话 ID
     */
    private void checkCid(String cid) {
        if (cid == null || cid.isBlank()) {
            throw new IllegalArgumentException("cid must not be blank");
        }
    }

    /**
     * 将历史查询条数限制在安全范围内。
     *
     * @param limit 客户端请求条数
     * @return 归一化后的查询条数
     */
    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_HISTORY_LIMIT;
        }
        return Math.min(limit, MAX_HISTORY_LIMIT);
    }
}
