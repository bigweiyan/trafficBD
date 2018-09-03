package com.hitbd.proj.model;

import com.hitbd.proj.Settings;
import com.hitbd.proj.util.Utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Device implements IDevice {
    private int userBId;
    private long imei;
    private String deviceType;
    private String projectId;
    private boolean enabled;
    private boolean repayment;
    private String deviceName;
    List<Pair<Integer, Date>> expireList;
    private int user_c_id;

    public Device(int userBId, long imei, String deviceType, String deviceName) {
        super();
        this.userBId = userBId;
        this.imei = imei;
        this.deviceType = deviceType;
        this.deviceName = deviceName;
    }

    public Device(int userBId, long imei) {
        super();
        this.userBId = userBId;
        this.imei = imei;
    }

    /**
     * @return imei
     */
    public long getImei() {
        return imei;
    }

    /**
     * @param imei 要设置的 imei
     */
    public void setImei(long imei) {
        this.imei = imei;
    }

    /**
     * @param enabled 要设置的 enabled
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @param repayment 要设置的 repayment
     */
    public void setRepayment(boolean repayment) {
        this.repayment = repayment;
    }

    @Override
    public int getUserBId() {
        return this.userBId;
    }

    @Override
    public void setUserBId(int bId) {
        this.userBId = bId;
    }

    @Override
    public int getUserCId() {
        return this.user_c_id;
    }

    @Override
    public void setUserCId(int Cid) {
        this.user_c_id = Cid;
    }

    @Override
    public String getDeviceType() {
        return deviceType;
    }

    @Override
    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    @Override
    public String getDeviceName() {
        return deviceName;
    }

    @Override
    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    @Override
    public String getProjectId() {
        return new String(this.projectId);
    }

    @Override
    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    @Override
    public boolean getEnabled() {
        return this.enabled;
    }

    @Override
    public boolean getRepayment() {
        return this.repayment;
    }

    @Override
    public List<Pair<Integer, Date>> getExpireList() {
        return this.expireList;
    }

    public String getExpireListText() {
        if (expireList == null) return null;
        StringBuilder sb = new StringBuilder();
        for (Pair<Integer, Date> pair : expireList) {
            sb.append(pair.getKey()).append(",").append(Utils.getRelativeDate(pair.getValue())).append(",");
        }
        if (sb.length() > 0) sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public void setExpireListByText(String expireList) {
        if (expireList == null || expireList.isEmpty()) {
            this.expireList = new ArrayList<>();
            return;
        }
        String[] list = expireList.split(",");
        if (list.length % 2 != 0) {
            throw new IllegalArgumentException("the argument should be even");
        }
        List<Pair<Integer, Date>> expireDate = new ArrayList<>();
        for (int i = 0; i < list.length; i+=2) {
            expireDate.add(new Pair<>(Integer.parseInt(list[i]),
                    new Date(Settings.BASETIME + 1000L * Integer.parseInt(list[i + 1]) * 3600 * 24)));
        }
        this.expireList = expireDate;
    }

    @Override
    public void setExpireList(List<Pair<Integer, Date>> expireList) {
        this.expireList = expireList;
    }

}
