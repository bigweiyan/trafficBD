package com.hitbd.proj.model;

import java.util.ArrayList;
import java.util.List;

import com.hitbd.proj.model.IUserC;

public class UserC<E> implements IUserC {
    private int user_id;
    private List<Long> devices;
    private List<Long> authedDevices;
    private List<Integer> authUserIds;

    public UserC() {}

    public UserC(int user_id, String devices, String authed_device, String auth_user_ids) {
        this.user_id = user_id;
        this.devices = longString2List(devices);
        this.authedDevices = longString2List(authed_device);
        this.authUserIds = intString2List(auth_user_ids);
    }

    @Override
    public int getUserCId() {
        return user_id;
    }

    @Override
    public void setUserCid(int user_id) {
        this.user_id = user_id;
    }

    @Override
    public List<Long> getDevices() {
        return devices;
    }

    public String getDevicesText() {
        return longList2String(devices);
    }

    @Override
    public void setDevices(List<Long> devices) {
        this.devices = devices;
    }

    public void setDevicesByText(String devices) {
        this.devices = longString2List(devices);
    }

    @Override
    public List<Long> getAuthedDevices() {
        return authedDevices;
    }

    public String getAuthedDevicesText() {
        return longList2String(authedDevices);
    }

    @Override
    public void setAuthedDevices(List<Long> authedDevices) {
        this.authedDevices = authedDevices;
    }

    public void setAuthedDevicesByText(String authedDevices){
        this.authedDevices = longString2List(authedDevices);
    }

    @Override
    public List<Integer> getAuthUserIds() {
        return authUserIds;
    }

    public String getAuthUserIdsText() {
        return intList2String(authUserIds);
    }

    @Override
    public void setAuthUserIds(List<Integer> authUserIds) {
        this.authUserIds = authUserIds;
    }

    public void setAuthUserIdsByText(String authUserIds) {
        this.authUserIds = intString2List(authUserIds);
    }

    private List<Long> longString2List(String s){
        if (s == null || s.isEmpty()) return new ArrayList<>();
        String[] list = s.split(",");
        List<Long> result = new ArrayList<>();
        for (String i : list) {
            result.add(Long.valueOf(i));
        }
        return result;
    }

    private List<Integer> intString2List(String s){
        if (s == null || s.isEmpty()) return new ArrayList<>();
        String[] list = s.split(",");
        List<Integer> result = new ArrayList<>();
        for (String i : list) {
            result.add(Integer.valueOf(i));
        }
        return result;
    }

    private String intList2String(List<Integer> l) {
        StringBuilder sb = new StringBuilder();
        if (l != null) {
            for (Integer i : l) {
                sb.append(i).append(",");
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
        }
        return sb.toString();
    }

    private String longList2String(List<Long> l) {
        StringBuilder sb = new StringBuilder();
        if (l != null) {
            for (Long i : l) {
                sb.append(i).append(",");
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
        }
        return sb.toString();
    }
}
