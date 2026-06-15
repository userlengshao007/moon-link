package com.moon.link.register;

import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.Set;

/**
 * 注册中心监听器接口，用于监听服务实例的变化事件。
 * 当注册中心的服务实例列表发生变更时，触发相应的回调通知。
 */
public interface RegisterCenterListener {
    /**
     * 当服务实例列表发生变化时的回调方法。
     *
     * @param instances 更新后的服务实例集合，包含所有当前可用的服务实例信息
     */
    void onInstancesChange(Set<Instance> instances);
}