package com.hitbd.proj.model;

import java.util.Date;
import com.hitbd.proj.util.Utils;

public class AlarmImpl implements IAlarm {
    
    private String id;
    private long imei;
    private String status;
    private String type;
    private float longitude;
    private float latitude;
    private float velocity;
    private String address;
    private Date createTime;
    private Date pushTime;
    private boolean viewed;
    private String encId;
    private String rowKey;
    
    public AlarmImpl() { }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getImei() {
        return this.imei;
    }

    public void setImei(long imei) {
        this.imei = imei;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public float getLongitude() {
        return this.longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public float getLatitude() {
        return this.latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getVelocity() {
        return this.velocity;
    }

    public void setVelocity(float velocity) {
        this.velocity = velocity;
    }

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(Date date) {
        this.createTime = date;
    }

    public Date getPushTime() {
        return this.pushTime;
    }

    public void setPushTime(Date date) {
        this.pushTime = date;
    }

    public boolean isViewed() {
        return this.viewed;
    }

    public void setViewed(boolean viewed) {
        this.viewed = viewed;
    }

    public String getEncId() {
        return this.encId;
    }

    public void setEncId(String encId) {
        this.encId = encId;
    }

    public String getRowKey() {
        return this.rowKey;
    }
    
    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getTableName() {
        return Utils.getTableName(this.createTime);
    }
}
