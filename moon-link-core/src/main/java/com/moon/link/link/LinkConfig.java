package com.moon.link.link;

/**
 * 当前长连接节点的运行时标识。
 */
public final class LinkConfig {
    /** 当前节点编号；使用 volatile 保证初始化结果对工作线程可见。 */
    public static volatile int MACHINE_ID = 1;

    private LinkConfig() {
    }
}
