package com.hitbd.proj.model.user_C;

import static org.junit.Assert.*;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;

public class userCtableTest {

  @Before
  public void setUp() throws Exception {}

  @Test
  public void test() throws Exception {
    userCtable uc = new userCtable();
    Connection conn=uc.ign.getConnect();
    Statement stmt = conn.createStatement();
    //uc.delect();
    //uc.createTable();
    user_C a= new user_C(1,"2121","323213","44324324");
    user_C b= new user_C(2,"2121","323213","44324324");
    user_C c= new user_C(3,"2121","323213","44324324");
    user_C d= new user_C(4,"2121","323213","44324324");
    user_C e= new user_C(5,"2121","323213","44324324");
    //uc.addData(a, conn);
    //uc.addData(b, conn);
    //uc.addData(c, conn);
    //uc.addData(d, conn);
    //uc.addData(e, conn);
    
    String selectsql = "SELECT * FROM user_C";
    ResultSet rs = stmt.executeQuery(selectsql);
    
    //PrintWriter out = new PrintWriter(file);
    while (rs.next()) {
      // Retrieve by column name
      int user_id = rs.getInt("user_id");
      String devices = rs.getString("devices");
      String authed_device = rs.getString("authed_device");
      String auth_user_ids = rs.getString("auth_user_ids");
      // Display values
      System.out.print("userC_id: " + user_id);
      System.out.print(", parent_id: " + devices);
      System.out.print(", children_ids: " + authed_device);
      System.out.println(", auth_user_ids: " + auth_user_ids);
      //String userCinfo =
         // String.format("user_id: %d, devices: %s, authed_device: %s, auth_user_ids: %s", user_id,
            //  devices, authed_device, auth_user_ids);
      //out.println(userCinfo);
    }
  }

}
