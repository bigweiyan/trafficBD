package com.hitbd.proj.model;

import java.util.List;

public interface IUserC {

    int getUserCId();

    void setUserCid(int id);

    /**
     * 获取该c端用户拥有的设备列表
     * @return
     */
    List<Long> getDevices();

    /**
     * 设置该c端用户拥有的设备列表
     * @param devices
     */
    void setDevices(List<Long> devices);

    /**
     * 获取该用户的授权访问设备
     * @return
     */
    List<Long> getAuthedDevices();

    /**
     * 设置该用户的授权访问设备
     * @param authedDevices
     */
    void setAuthedDevices(List<Long> authedDevices);

    /**
     * 获取该用户授权的用户列表
     * @return
     */
    List<Integer> getAuthUserIds();

    /**
     * 设置该用户授权的用户列表
     * @param authUserIds
     */
    void setAuthUserIds(List<Integer> authUserIds);
}
