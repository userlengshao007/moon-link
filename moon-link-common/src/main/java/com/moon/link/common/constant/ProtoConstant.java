package com.moon.link.common.constant;

/**
 * 自定义消息协议的固定参数。
 */
public final class ProtoConstant {
    /** 协议魔数，占 2 字节，用于快速识别非法数据帧。 */
    public static final short MAGIC = (short) 0xCAFE;
    /** 协议版本号，占 2 字节。 */
    public static final short VERSION = 1;
    /** 不包含消息体的固定头长度。 */
    public static final int FIXED_HEADER_LENGTH = 12;
    /** 单个数据帧允许的最大长度，防止异常报文占用过多内存。 */
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;
    /** 未显式配置密钥时使用的默认 AES 密钥。 */
    public static final String DEFAULT_SECRETKEY = "DEFAULTSECRETBLG";

    private ProtoConstant() {
    }
}
