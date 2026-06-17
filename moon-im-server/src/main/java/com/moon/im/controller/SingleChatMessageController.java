package com.moon.im.controller;

import com.moon.im.domain.SingleChatMessage;
import com.moon.im.service.SingleChatMessageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/single")
public class SingleChatMessageController {

    private final SingleChatMessageService singleChatMessageService;

    public SingleChatMessageController(SingleChatMessageService singleChatMessageService) {
        this.singleChatMessageService = singleChatMessageService;
    }

    @GetMapping("/messages")
    public List<SingleChatMessage> listBySeqRange(@RequestParam String cid,
                                                  @RequestParam Long startSeqId,
                                                  @RequestParam Long endSeqId) {
        return singleChatMessageService.listBySeqRange(cid, startSeqId, endSeqId);
    }

    @GetMapping("/history")
    public List<SingleChatMessage> listHistory(@RequestParam String cid,
                                               @RequestParam(required = false) Long beforeSeqId,
                                               @RequestParam(defaultValue = "20") Integer limit) {
        return singleChatMessageService.listHistory(cid, beforeSeqId, limit);
    }
}
