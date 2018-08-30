package com.hitbd.proj.model.user_C;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.hitbd.proj.model.device.device;
import com.hitbd.proj.model.exceptions.userCnameExists;
import com.hitbd.proj.model.user_B.ignite;


public class userCtable {
  public static ignite ign=new ignite();
  /**
   * 对user_c表添加数据
   * @param employee
   * @return
   * @throws Exception
   */
  public static int addData(user_C usr,Connection conn) throws Exception{
      //Connection conn = ign.getConnect();
      String sql = "insert into user_C values(?,?,?,?)";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, usr.getUserCId());
      pstmt.setString(2, usr.getDevicesString());
      pstmt.setString(3,usr.getAuthed_deviceString());
      pstmt.setString(4,usr.getAuth_user_idsString());
      int result = pstmt.executeUpdate();
      //ign.disConnect(conn);
      return result;
  }
  /**
   * 对user_c表内容进行修改
   * @param employee
   * @return
   * @throws Exception 
   */
  public static int updateData(user_C usr) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "update user_C set devices=?,authed_device=? ,auth_user_ids= ? where user_id=?";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(4, usr.getUserCId());
      pstmt.setString(1, usr.getDevicesString());
      pstmt.setString(2, usr.getAuthed_deviceString());
      pstmt.setString(3, usr.getAuth_user_idsString());
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  
  /**
   * 对user_c内容删除
   * @param employee
   * @return
   * @throws Exception
   */
  public static int deleteData(user_C usr) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "delete from user_C where user_id=? ";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, usr.getUserCId());
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  public static void delect() throws SQLException { 
    Connection conn = ign.getConnect();
    String sql = "DROP TABLE user_C";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
  /*public static void insert(user_C usera,user_C userb) throws SQLException {
    Connection conn = ign.getConnect();
    String sql = "update user_C set authed_device=? ,auth_user_ids= ? where user_id=?";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(3, usera.getuser_Id());
    pstmt.setString(2, usera.getAuth_user_ids()+","+userb.getuser_Id());
    pstmt.setString(1, usera.getAuthed_device()+","+userb.getAuthed_device());
    pstmt.executeUpdate();
  }*/
  public static void addDevice(user_C user,device dev) throws SQLException {
    Connection conn = ign.getConnect();
    String sql = "update user_C set authed_device=? where user_id=?";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setString(1, user.getAuthed_deviceString()+","+dev.getImei());
    
    pstmt.executeUpdate();
  }
  /*public static void insert(int user_id) throws SQLException {
    Connection conn = ign.getConnect();
    String sql = "insert into user_C(user_id) select "+user_id+
        "where NOT EXISTS(SELECT user_C.user_id , user_B.user_id FROM user_B,user_C WHERE user_C.user_id != ? AND user_B.user_id = ?)";
    String sql1="SELECT user_C.user_id , user_B.user_id FROM user_B,user_C WHERE user_C.user_id = ? OR user_B.user_id = ?";
    PreparedStatement pstmt = conn.prepareStatement(sql1);
    pstmt.setInt(1, user_id);pstmt.setInt(2, user_id);
    ResultSet rs=pstmt.executeQuery();
    if(!rs.isBeforeFirst()) {
      String sql2 = "insert into user_C values(?,null,null,null)";
      PreparedStatement stmt = conn.prepareStatement(sql2);
      stmt.setInt(1, user_id);
      stmt.executeUpdate();
    }
    else {
      throw new userCnameExists(user_id);
    }
  }*/
  public static void createTable() throws SQLException {
    Connection conn = ign.getConnect();
    String sql="CREATE TABLE user_C " +
        "(user_id INTEGER  NULL," +
        "devices VARCHAR(50) , "+
        "authed_device VARCHAR(50),"+
        "auth_user_ids VARCHAR(50),"+
        "PRIMARY KEY (user_id))";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
}
