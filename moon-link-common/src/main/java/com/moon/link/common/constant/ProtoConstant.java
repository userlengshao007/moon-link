package com.moon.link.common.constant;

public class ProtoConstant {
    // 2 字节
    public static final short MAGIC = (short) 0xCAFE;
    // 2 字节
    public static final short VERSION = 1;

    public static final int FIXED_HEADER_LENGTH = 12;

    public static final int MAX_FRAME_LENGTH = 1024 * 1024;

    public static final String DEFAULT_SECRETKEY = "DEFAULTSECRETBLG";
}
