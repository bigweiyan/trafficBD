package com.hitbd.proj.model.device;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.hitbd.proj.model.IDevice;
import com.hitbd.proj.model.Pair;

public class device implements IDevice{
private  int user_b_id;
private  long imei; 
private  StringBuffer device_type;
private  StringBuffer undifined;
private  StringBuffer project_id;
private  boolean enabled;
private  boolean repayment;
private  StringBuffer expire_list;
List<Pair<Integer, Date>> expireList;
private int  user_c_id;
public device(int user_b_id,long imei,String device_type,String undifined) {
  super();
  this.user_b_id=user_b_id;
  this.imei=imei;
  this.device_type=new StringBuffer(device_type);
  this.setUndifined(undifined); 
}
public device(int user_b_id,long imei) {
  super();
  this.user_b_id=user_b_id;
  this.imei=imei;
}
public  int getUser_id() {
  return user_b_id;
}
public  void setUser_id(int user_id) {
  this.user_b_id = user_id;
}
/**
 * @return imei
 */
public  long getImei() {
  return imei;
}
/**
 * @param imei 要设置的 imei
 */
public  void setImei(long imei) {
  this.imei = imei;
}
/**
 * @return device_type
 */
public  String getDevice_type() {
  return new String(device_type);
}
/**
 * @param device_type 要设置的 device_type
 */
public  void setDevice_type(String device_type) {
  this.device_type = new StringBuffer(device_type);
}
/**
 * @return project_id
 */
public  String getProject_id() {
  return new String(project_id);
}
/**
 * @param project_id2 要设置的 project_id
 */
public  void setProject_id(StringBuffer project_id2) {
  this.project_id = new StringBuffer(project_id2);
}
/**
 * @return enabled
 */
public  boolean isEnabled() {
  return enabled;
}
/**
 * @param enabled 要设置的 enabled
 */
public  void setEnabled(boolean enabled) {
  this.enabled = enabled;
}
/**
 * @return repayment
 */
public  boolean isRepayment() {
  return repayment;
}
/**
 * @param repayment 要设置的 repayment
 */
public  void setRepayment(boolean repayment) {
  this.repayment = repayment;
}
/**
 * @return expire_list
 */
public  String getExpire_list() {
  return new String(expire_list);
}
/**
 * @param expire_list 要设置的 expire_list
 */
public  void setExpire_list(String expire_list) {
  this.expire_list = new StringBuffer(expire_list);
}
/**
 * @return user_c_id
 */
public int getUser_c_id() {
  return user_c_id;
}
/**
 * @param user_c_id 要设置的 user_c_id
 */
public void setUser_c_id(int user_c_id) {
  this.user_c_id = user_c_id;
}
/**
 * @return undifined
 */
public String getUndifined() {
  return new String(undifined);
}
/**
 * @param undifined 要设置的 undifined
 */
public void setUndifined(String undifined) {
  this.undifined = new StringBuffer(undifined);
}
@Override
public int getUserBId() {
  // TODO 自动生成的方法存根
  return this.user_b_id;
}
@Override
public void setUserBId(int bId) {
  // TODO 自动生成的方法存根
  this.user_b_id=bId;
}
@Override
public int getUserCId() {
  // TODO 自动生成的方法存根
  return this.user_c_id;
}
@Override
public void setUserCId(int Cid) {
  // TODO 自动生成的方法存根
  this.user_c_id=Cid;
}
@Override
public String getDeviceType() {
  // TODO 自动生成的方法存根
  return new String(this.device_type);
}
@Override
public void setDeviceType(String deviceType) {
  // TODO 自动生成的方法存根
  this.device_type=new StringBuffer(deviceType);
}
@Override
public String getDeviceName() {
  // TODO 自动生成的方法存根
  return new String(this.undifined);
}
@Override
public void setDeviceName(String deviceName) {
  // TODO 自动生成的方法存根
  this.undifined=new StringBuffer(deviceName);
}
@Override
public String getProjectId() {
  // TODO 自动生成的方法存根
  return new String(this.project_id);
}
@Override
public void setProjectId(String projectId) {
  // TODO 自动生成的方法存根
  this.project_id=new StringBuffer(projectId);
}
@Override
public boolean getEnabled() {
  // TODO 自动生成的方法存根
  return this.enabled;
}
@Override
public boolean getRepayment() {
  // TODO 自动生成的方法存根
  return this.repayment;
}
@Override
public List<Pair<Integer, Date>> getExpireList() {
  return this.expireList;
}
@Override
public void setExpireList(List<Pair<Integer, Date>> expireList) {
  // TODO 自动生成的方法存根
  this.expireList=expireList;
}

}
