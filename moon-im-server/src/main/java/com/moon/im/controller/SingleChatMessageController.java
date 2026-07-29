package com.moon.im.controller;

import com.moon.im.domain.SingleChatMessage;
import com.moon.im.service.SingleChatMessageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 单聊消息查询接口。
 */
@RestController
@RequestMapping("/single")
public class SingleChatMessageController {

    private final SingleChatMessageService singleChatMessageService;

    /**
     * 创建消息查询控制器。
     *
     * @param singleChatMessageService 单聊消息服务
     */
    public SingleChatMessageController(SingleChatMessageService singleChatMessageService) {
        this.singleChatMessageService = singleChatMessageService;
    }

    /**
     * 按消息序号闭区间查询会话消息。
     *
     * @param cid 会话 ID
     * @param startSeqId 起始序号
     * @param endSeqId 结束序号
     * @return 区间内消息
     */
    @GetMapping("/messages")
    public List<SingleChatMessage> listBySeqRange(@RequestParam String cid,
                                                  @RequestParam Long startSeqId,
                                                  @RequestParam Long endSeqId) {
        return singleChatMessageService.listBySeqRange(cid, startSeqId, endSeqId);
    }

    /**
     * 游标式查询指定会话的历史消息。
     *
     * @param cid 会话 ID
     * @param beforeSeqId 游标序号；首次查询可不传
     * @param limit 返回条数
     * @return 一页历史消息
     */
    @GetMapping("/history")
    public List<SingleChatMessage> listHistory(@RequestParam String cid,
                                               @RequestParam(required = false) Long beforeSeqId,
                                               @RequestParam(defaultValue = "20") Integer limit) {
        return singleChatMessageService.listHistory(cid, beforeSeqId, limit);
    }
}
