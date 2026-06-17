package com.moon.im.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("single_chat_message")
public class SingleChatMessage {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String cid;

    private Long seqId;

    private Long fromUserId;

    private Long toUserId;

    private String content;

    private Integer messageType;

    private Integer status;

    private String kafkaTopic;

    private Integer kafkaPartition;

    private Long kafkaOffset;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
