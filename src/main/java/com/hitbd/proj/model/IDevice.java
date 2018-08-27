package com.hitbd.proj.model;

import java.util.Date;
import java.util.List;

public interface IDevice {

    int getUserBId();

    void setUserBId(int bId);

    /**
     * 获取与设备相连的C端用户ID，如果没有相应用户，则返回-1
     * @return
     */
    int getUserCId();

    /**
     * 设置与设备相连的C端用户ID，如果没有相应用户，可以设置为-1
     * @param Cid
     */
    void setUserCId(int Cid);

    long getImei();

    void setImei(long imei);

    String getDeviceType();

    void setDeviceType(String deviceType);

    String getDeviceName();

    void setDeviceName(String deviceName);

    String getProjectId();

    void setProjectId(String projectId);

    boolean getEnabled();

    void setEnabled(boolean enabled);

    boolean getRepayment();

    void setRepayment(boolean repayment);

    /**
     * 获取过期时间列表
     * @return
     */
    List<Pair<Integer, Date>> getExpireList();

    /**
     * 设置过期时间列表
     * @param expireList
     */
    void setExpireList(List<Pair<Integer, Date>> expireList);
}
