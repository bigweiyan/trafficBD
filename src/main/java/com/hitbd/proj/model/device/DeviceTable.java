package com.hitbd.proj.model.device;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import ignite.ignite;


public class DeviceTable {
  public static ignite ign=new ignite();
  /**
   * 对user_b表添加数据
   * @param employee
   * @return
   * @throws Exception
   */
  public static int addData(device dev,Connection conn) throws Exception{
      //Connection conn = ign.getConnect();
      String sql = "insert into device(user_b_id,imei,device_type,undifined,project_id,enabled,repayment,expire_list,user_c_id) values(?,?,?,?,?,?,?,?,?)";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setLong(2, dev.getImei());
      pstmt.setString(3, dev.getDevice_type());
      pstmt.setString(4, dev.getUndifined());
      pstmt.setInt(1, dev.getUser_id());
      pstmt.setString(5, dev.getProject_id());
      pstmt.setBoolean(6, dev.isEnabled());
      pstmt.setBoolean(7, dev.isRepayment());
      pstmt.setString(8,dev.getExpire_list());
      pstmt.setInt(9, dev.getUser_c_id());
      int result = pstmt.executeUpdate();
      //ign.disConnect(conn);
      return result;
  }
  /**
   * 对user_b表内容进行修改
   * @param employee
   * @return
   * @throws Exception 
   */
  public static int updateData(device dev) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "update device set imei=?,device_type=? ,undifined= ? ,project_id=?,enabled=?,repayment=?,expire_list=?,user_c_id=?,where user_b_id=?";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setLong(1, dev.getImei());
      pstmt.setString(2, dev.getDevice_type());
      pstmt.setString(3, dev.getUndifined());
      pstmt.setInt(4, dev.getUser_id());
      pstmt.setBoolean(5, dev.isEnabled());
      pstmt.setBoolean(6, dev.isRepayment());
      pstmt.setString(7,dev.getExpire_list());
      pstmt.setInt(8, dev.getUser_c_id());
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  
  /**
   * 对user_b内容删除
   * @param employee
   * @return
   * @throws Exception
   */
  public static int deleteData(device dev) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "delete from device where user_b_id=? ";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dev.getUser_id());
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  /**
   * 对device 表删除
   * @param employee
   * @return
   * @throws Exception
   */
  public static void delect() throws SQLException { 
    Connection conn = ign.getConnect();
    String sql = "DROP TABLE device";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
  public void insert(int userB_id,long imei) throws SQLException {
    Connection conn = ign.getConnect();
    try { 
      String sqlid="SELECT user_id FROM user_B WHERE user_id = ?";
      PreparedStatement pstmt = conn.prepareStatement(sqlid);
      pstmt.setInt(1, userB_id);
      ResultSet rs=pstmt.executeQuery();
      if(rs.isBeforeFirst()) {
        String sqlimei="SELECT imei FROM device WHERE imei = ?";
        PreparedStatement pstmtimei = conn.prepareStatement(sqlimei);
        pstmtimei.setLong(1, imei);
        ResultSet rsimei=pstmtimei.executeQuery();
        if(!rsimei.isBeforeFirst()) {
        String sql2 = "insert into device(user_b_id,imei) values(?,?)";
        PreparedStatement stmt = conn.prepareStatement(sql2);
        stmt.setInt(1, userB_id);
        stmt.setLong(2, imei);
        stmt.executeUpdate();
        }
        else {
          throw new imeiExists(imei);
        }
      }
      else {
        throw new noUserB(userB_id);
      }
    }
    catch(noUserB e) {
      
    }
   
  }
  public static void createTable() throws SQLException {
    Connection conn = ign.getConnect();
    String sql="CREATE TABLE device " +
        "(user_b_id INTEGER not NULL," +
        "imei LONG not NULL, "+
        "device_type VARCHAR(50),"+
        "undifined VARCHAR(50),"+
        "project_id VARCHAR(50),"+
        "enabled TINYINT,"+
        "repayment TINYINT,"+
        "expire_list VARCHAR(50),"+
        "user_c_id INTEGER  NULL,"+
        "PRIMARY KEY (imei))";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
}
