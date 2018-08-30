package com.hitbd.proj.model.user_B;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class userBtable {
  public ignite ign=new ignite();
  /**
   * 对user_b表添加数据
   * @param employee
   * @return
   * @throws Exception
   */
  public int addData(user_B usr,Connection conn) throws Exception{
      String sql = "insert into user_b values(?,?,?)";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, usr.getUserBId());
      pstmt.setInt(2, usr.getParentId());
      pstmt.setString(3,usr.getChildren());
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
  public  int updateData(user_B usr) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "update user_b set parent_id=?,children_ids=? where user_id=?";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(3, usr.getUserBId());
      pstmt.setInt(1, usr.getParentId());
      pstmt.setString(2, new String(usr.children_ids));
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  public void changeRelationship(user_B usrA,user_B usrB) throws SQLException {
    Connection conn = ign.getConnect();
    String sql1 = "update user_b set parent_id=? where user_id=?";
    PreparedStatement pstmt1 = conn.prepareStatement(sql1);
    pstmt1.setInt(1, usrA.getUserBId());
    pstmt1.setInt(2, usrB.getUserBId());
    pstmt1.executeUpdate();  
    String sql2 = "update user_b set children_ids=? where user_id=?";
    PreparedStatement pstmt2 = conn.prepareStatement(sql2);
    pstmt2.setString(1, usrA.getChildren()+","+usrB.getUserBId());
    pstmt2.setInt(2, usrA.getUserBId());
    pstmt2.executeUpdate();
    ign.disConnect(conn);
  }
  /**
   * 对user_b内容删除
   * @param employee
   * @return
   * @throws Exception
   */
  public  int deleteData(user_B usr) throws Exception{
      Connection conn = ign.getConnect();
      String sql = "delete from user_B where user_id=? ";
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, usr.getUserBId());
      int result = pstmt.executeUpdate();
      ign.disConnect(conn);
      return result;
  }
  public  int deleteUserB(int usrid) throws Exception{
    Connection conn = ign.getConnect();
    String sql = "delete from user_B where user_id=? ";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(1, usrid);
    int result = pstmt.executeUpdate();
    ign.disConnect(conn);
    return result;
}
  /*public void insert(int user_id,int parentid) throws SQLException {
    Connection conn = ign.getConnect();
    String testid="select user_id from user_B where user_id =?";
    String testpar="select user_id from user_B where user_id =?";
    //String insert = "INSERT INTO user_B(user_id, parent_id) SELECT "+user_id+","+parentid
        //+ "FROM DUAL WHERE EXISTS(SELECT user_id FROM user_B WHERE user_id != ? AND user_id = ?);";
    try {
      PreparedStatement pstmtu = conn.prepareStatement(testid);
      pstmtu.setInt(1, user_id);
      ResultSet rs1 = pstmtu.executeQuery();
      if(!rs1.isBeforeFirst()) {
        PreparedStatement pstmtp = conn.prepareStatement(testpar);
        pstmtp.setInt(1, parentid);
        ResultSet rs2 = pstmtp.executeQuery();
        if(rs2.isBeforeFirst()) {
          String insert ="INSERT INTO user_B values(?,?,null)";
          PreparedStatement pstmtinsert = conn.prepareStatement(insert);
          pstmtinsert.setInt(1, user_id);
          pstmtinsert.setInt(2, parentid);
          int re=pstmtinsert.executeUpdate();
        }
        else {
          throw new noParent(parentid);
        }
      }
      else {
        throw new useridExisits(user_id);
      }
    }
    catch(useridExisits e) {
      
    }
    catch(noParent e) {
      
    }
    finally{
      conn.close();
    }
  }*/
  public  void delect() throws SQLException { 
    Connection conn = ign.getConnect();
    String sql = "DROP TABLE user_B";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
  public  void createTable() throws SQLException {
    Connection conn = ign.getConnect();
    String sql="CREATE TABLE user_B " +
        "(user_id INTEGER not NULL," +
        "parent_id INTEGER , "+
        "children_ids VARCHAR(4096),"+
        "PRIMARY KEY (user_id))";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.executeUpdate();
  }
  public void addDataNUll(user_B user, Connection conn) throws SQLException {
    String sql = "insert into user_b values(?,null,?)";
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(1, user.getUserBId());
    //pstmt.setInt(2, usr.getParent_id());
    pstmt.setString(2,user.getChildrenString());
    int result = pstmt.executeUpdate();
  }
}
