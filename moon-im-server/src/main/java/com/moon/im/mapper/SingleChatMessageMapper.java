package com.moon.im.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.moon.im.domain.SingleChatMessage;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SingleChatMessageMapper extends BaseMapper<SingleChatMessage> {
}
