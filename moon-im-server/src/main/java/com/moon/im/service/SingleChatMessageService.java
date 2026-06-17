package com.moon.im.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.moon.im.domain.SingleChatMessage;

import java.util.List;

public interface SingleChatMessageService extends IService<SingleChatMessage> {

    List<SingleChatMessage> listBySeqRange(String cid, long startSeqId, long endSeqId);

    List<SingleChatMessage> listHistory(String cid, Long beforeSeqId, int limit);

    List<SingleChatMessage> listUnpushedMessages(long toUserId, int limit);

    void updatePushStatus(Long id, int status);
}
