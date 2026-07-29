package com.moon.im.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.moon.im.domain.SingleChatMessage;

import java.util.List;

/**
 * 单聊消息的查询、补推和状态更新服务。
 */
public interface SingleChatMessageService extends IService<SingleChatMessage> {

    /**
     * 查询会话内指定序号区间的消息。
     *
     * @param cid 会话 ID
     * @param startSeqId 起始序号，包含边界
     * @param endSeqId 结束序号，包含边界
     * @return 按序号升序排列的消息
     */
    List<SingleChatMessage> listBySeqRange(String cid, long startSeqId, long endSeqId);

    /**
     * 向前分页查询会话历史消息。
     *
     * @param cid 会话 ID
     * @param beforeSeqId 当前页之前的消息序号；首页可传 {@code null}
     * @param limit 期望返回条数
     * @return 按序号升序排列的历史消息
     */
    List<SingleChatMessage> listHistory(String cid, Long beforeSeqId, int limit);

    /**
     * 查询指定用户尚未成功推送的消息。
     *
     * @param toUserId 接收者用户 ID
     * @param limit 最大返回条数
     * @return 待补推消息列表
     */
    List<SingleChatMessage> listUnpushedMessages(long toUserId, int limit);

    /**
     * 更新消息推送状态。
     *
     * @param id 消息主键
     * @param status 新推送状态
     */
    void updatePushStatus(Long id, int status);
}
