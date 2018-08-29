package com.hitbd.proj.model.fileamanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import com.hitbd.proj.IIgniteSearch;
import com.hitbd.proj.Exception.DuplicatedPKException;
import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.IDevice;
import com.hitbd.proj.model.IUserB;
import com.hitbd.proj.model.IUserC;
import com.hitbd.proj.model.TreeNode.Tree;
import com.hitbd.proj.model.device.DeviceTable;
import com.hitbd.proj.model.device.device;
import com.hitbd.proj.model.igniteinfo.Alarm_c;
import com.hitbd.proj.model.igniteinfo.Viewed_c;
import com.hitbd.proj.model.randomDate.Randomdate;
import com.hitbd.proj.model.user_B.userBtable;
import com.hitbd.proj.model.user_B.user_B;
import com.hitbd.proj.model.user_C.userCtable;
import com.hitbd.proj.model.user_C.user_C;


public class manager implements IIgniteSearch{ 
  Connection connection = null;
  DeviceTable device = new DeviceTable();
  userBtable ub = new userBtable();
  userCtable uc = new userCtable();
  Tree tre = new Tree();
  ArrayList<String> deviceIMEI = new ArrayList<String>();
  File file = new File("userC.txt");
  Viewed_c vc=new Viewed_c();
  Alarm_c ac=new Alarm_c();
  public void read(String filepath) throws Exception {
    this.connect();
    Statement stmt = connection.createStatement();
    DeviceTable.delect();
    DeviceTable.createTable();
    File file = new File(filepath);
    Scanner sc = null;
    try {
      sc = new Scanner(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    String tempString = null;
    while (sc.hasNextLine()) {
      tempString = sc.nextLine();
      int user_b_id = 0;
      long imei = 0;
      String device_type = null;
      String undifined = null;
      boolean enabled = true;
      boolean repayment = true;
      StringBuffer project_id = null;
      StringBuffer user_parent_id = null;
      StringBuffer expire_list = null;
      if (tempString.contains("\"")) {
        String[] temp = tempString.split(",", 9);
        project_id = new StringBuffer(temp[0].replaceAll("\"", ""));
        enabled = temp[1].replaceAll("\"", "").equals("Y") ? true : false;
        deviceIMEI.add(temp[2].replaceAll("\"", ""));
        imei = Long.parseLong(temp[2].replaceAll("\"", ""));
        undifined = temp[3].replaceAll("\"", "");
        device_type = temp[4].replaceAll("\"", "");
        repayment = temp[6].replaceAll("\"", "").equals("1") ? true : false;
        user_b_id = Integer.parseInt(temp[7].replaceAll("\"", ""));
        user_parent_id = new StringBuffer(temp[8].replaceAll("\"", ""));
        /////////////////////////////////
        expire_list = new StringBuffer(createDate(new String(user_parent_id)));
        /////////////////////////////////
      }
      device dev = new device(user_b_id, imei, device_type, undifined);
      dev.setEnabled(enabled);
      dev.setRepayment(repayment);
      dev.setProject_id(project_id);
      dev.setExpire_list(new String(expire_list));
      DeviceTable.addData(dev, connection);
      tre.update(new String(user_parent_id));
    }
    sc.close();
    String selectsql = "SELECT * FROM device";
    ResultSet rs = stmt.executeQuery(selectsql);
    while (rs.next()) {
      // Retrieve by column name
      int user_id = rs.getInt("user_b_id");
      long parent_id = rs.getLong("imei");
      String children_ids = rs.getString("device_type");
      String project_id = rs.getString("project_id");
      String expire_list = rs.getString("expire_list");
      String undifined = rs.getString("undifined");
      boolean enabled = rs.getBoolean("enabled");
      boolean repayment = rs.getBoolean("repayment");
      // Display values
      System.out.print("user_id: " + user_id);
      System.out.print(", imei: " + parent_id);
      System.out.print(", device_type: " + children_ids);
      System.out.print(", project_id: " + project_id);
      System.out.print(", undifined: " + undifined);
      System.out.print(", enabled: " + enabled);
      System.out.print(", repayment: " + repayment);
      System.out.println(", expire_list: " + expire_list);
    }
    this.close();
  }

  public String createDate(String str) throws ParseException {
    String[] temp = str.replaceAll("\"", "").split(",");
    int len=temp.length;
    StringBuffer date = new StringBuffer();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date initaldate=format.parse("2010-01-01 00:00:00");
    Date randomdate=Randomdate.randomDate("2010-01-01 00:00:00", "2099-09-19 00:00:00");
    long initial=(randomdate.getTime()-initaldate.getTime())/(1000*3600*24);
    date.append(temp[len-1] +","+ initial+",");
    if(len>3) {
      for(int i=len-2;i>len-4;i--){
        date.append(temp[i]+","+(15*(len-1-i))+",");
        }
      for(int i=len-4;i>0;i--) {
        date.append(temp[i]+","+0+",");
      }
    }
    else {
      for(int i=len-2;i>0;i--){
      date.append(temp[i]+","+(15*(len-1-i))+",");
      }
    }
    return new String(date.deleteCharAt(date.length() - 1));
  }


  public void addUserC() throws Exception {
    this.connect();
    Statement stmt = connection.createStatement();
    //uc.delect();
    uc.createTable();
    user_C userC = null;
    int len = deviceIMEI.size();
    for (int i = 0; i < len; i++) {
      userC = new user_C(1000001 + i, deviceIMEI.get(i),
          deviceIMEI.get((int) (0 + Math.random() * (len))) + ","
              + deviceIMEI.get((int) (0 + Math.random() * (len))),
          deviceIMEI.get((int) (0 + Math.random() * (len))) + ","
              + deviceIMEI.get((int) (0 + Math.random() * (len))));
      uc.addData(userC, connection);
    }
    String selectsql = "SELECT * FROM user_C";
    ResultSet rs = stmt.executeQuery(selectsql);
    PrintWriter out = new PrintWriter(file);
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
      String userCinfo =
          String.format("user_id: %d, devices: %s, authed_device: %s, auth_user_ids: %s", user_id,
              devices, authed_device, auth_user_ids);
      out.println(userCinfo);
    }
    out.close();
    this.close();
  }


  public void addUserB() throws Exception {
    this.connect();
    Statement stmt = connection.createStatement();
    //ub.delect();
    ub.createTable();
    user_B userb = null;
    for (int i = 0; i < tre.Tre.size(); i++) {
      if (!(tre.Tre.get(i).parent == null)) {
        userb = new user_B(Integer.parseInt(tre.Tre.get(i).name),
            Integer.parseInt((tre.Tre.get(i).parent)), toString(tre.Tre.get(i).childset));
        ub.addData(userb, connection);
      } else {
        userb =
            new user_B(Integer.parseInt(tre.Tre.get(i).name), toString(tre.Tre.get(i).childset));
        ub.addDataNUll(userb, connection);
      }
    }
    String selectsql = "SELECT user_id,parent_id,children_ids FROM user_B";
    ResultSet rs = stmt.executeQuery(selectsql);
    while (rs.next()) {
      // Retrieve by column name
      int user_id = rs.getInt("user_id");
      int parent_id = rs.getInt("parent_id");
      String children_ids = rs.getString("children_ids");
      // Display values
      System.out.print("user_id: " + user_id);
      System.out.print(", parent_id: " + parent_id);
      System.out.println(", children_ids: " + children_ids);
    }
    this.close();
  }


  public String toString(Set<String> set) {
    StringBuffer temp = new StringBuffer();
    for (String str : set) {
      temp.append(str + ",");
    }
    int len=temp.length();
    return new String(temp).substring(0, len-1);
  }


  public static void main(String[] args) throws Exception {
    String pathfile = "report_alarm_8_device.txt";
    manager man = new manager();
    man.read(pathfile);
    man.addUserB();
    man.addUserC();
  }

  @Override
  public boolean connect() {
    try {
    vc.createCache();
    ac.createCache();
    connection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
    }catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public boolean connect(String hostname, int port) {
    try {
      connection = DriverManager.getConnection("jdbc:ignite:thin://" + hostname + ":" + port + "/");
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public int getAlarmCount(long imei) {
    // TODO 自动生成的方法存根
    return ac.getAlarmCount(imei);
  }

  @Override
  public void setAlarmCount(long imei, int count) {
    ac.setAlarmCount(imei, count);
    
  }

  @Override
  public int getViewedCount(long imei) {
    // TODO 自动生成的方法存根
    return vc.getViewedCount(imei);
  }

  @Override
  public void setViewedCount(long imei, int count) {
    // TODO 自动生成的方法存根
    vc.setViewedCount(imei, count);
  }

  @Override
  public IUserB getUserB(int userBId) throws SQLException, NotExistException {
    this.connect();
    String sql1="SELECT user_id FROM user_B WHERE user_id = ?";
    PreparedStatement pstmt = connection.prepareStatement(sql1);
    pstmt.setInt(1, userBId);
    ResultSet rs=pstmt.executeQuery();
    try {
    if(rs.isBeforeFirst()) {
      int user_id = rs.getInt("user_id");
      int parent_id = rs.getInt("parent_id");
      String children_ids = rs.getString("children_ids");
      return new user_B(user_id,parent_id,children_ids);
    }
    else {
      throw new NotExistException();
    }
    }
    catch(NotExistException e) {
      
    }
    finally{
      this.close();
    }
    return null;
  }

  @Override
  public IDevice getDevice(int imei) throws NotExistException, SQLException {
    this.connect();
    String sql1="SELECT imei FROM device WHERE imei = ?";
    PreparedStatement pstmt = connection.prepareStatement(sql1);
    pstmt.setLong(1, imei);
    ResultSet rs=pstmt.executeQuery();
    try {
    if(rs.isBeforeFirst()) {
      int user_b_id = rs.getInt("user_b_id");
      long Imei = rs.getInt("imei");
      String device_type = rs.getString("device_type");
      String undifined=rs.getString("undifined");
      return new device(user_b_id,Imei,device_type, undifined);
    }
    else {
      throw new NotExistException();
    }
    }
    catch(NotExistException e) {
      
    }
    finally{
      this.close();
    }
    return null;
  }

  @Override
  public IUserC getUserC(int userCId) throws SQLException  {
    this.connect();
    String sql1="SELECT user_id FROM user_C WHERE imei = ?";
    PreparedStatement pstmt = connection.prepareStatement(sql1);
    pstmt.setInt(1, userCId);
    ResultSet rs=pstmt.executeQuery();
    try {
    if(rs.isBeforeFirst()) {
      int user_id = rs.getInt("user_id");
      String devices = rs.getString("devices");
      String authed_device = rs.getString("authed_device");
      String auth_user_ids=rs.getString("auth_user_ids");
      return new user_C(user_id,devices,authed_device, auth_user_ids);
    }
    else {
      throw new NotExistException();
    }
    }
    catch(NotExistException e) {
      
    }
    finally{
      this.close();
    }
    return null;
  }

  @Override
  public int createUserB(int parentId) throws Exception {
    this.connect();
    String sql1="SELECT user_id FROM user_B WHERE user_id = ?";
    PreparedStatement pstmt = connection.prepareStatement(sql1);
    pstmt.setInt(1, parentId);
    ResultSet rs=pstmt.executeQuery();
    String sql2="SELECT user_id FROM user_B ";
    PreparedStatement pstmt2 = connection.prepareStatement(sql2);
    pstmt.setInt(1, parentId);
    ResultSet rs2=pstmt2.executeQuery();
    rs2.last();
    int newid=rs2.getInt("user_id")+1;
    if(rs.isBeforeFirst()) {
      user_B b=new user_B(newid, parentId);
      ub.addData(b, connection);
      this.close();
      return newid;
    }
    else {
      user_B b=new user_B(newid, -1);
      ub.addData(b, connection);
      this.close();
      return newid;
    }
  }

  @Override
  public int createUserC() throws Exception {
    this.connect();
    String sql="SELECT user_id FROM user_C ";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    ResultSet rs=pstmt.executeQuery();
    rs.last();
    int newid=rs.getInt("user_id")+1;
    user_C c=new user_C(newid, null, null, null);
    uc.addData(c, connection);
    this.close();
    return newid;
  }

  @Override
  public void createDevice(long imei, int userBId)
      throws DuplicatedPKException, ForeignKeyException, SQLException {
    this.connect();
    try { 
      String sqlid="SELECT user_id FROM user_B WHERE user_id = ?";
      PreparedStatement pstmt = connection.prepareStatement(sqlid);
      pstmt.setInt(1, userBId);
      ResultSet rs=pstmt.executeQuery();
      if(rs.isBeforeFirst()) {
        String sqlimei="SELECT imei FROM device WHERE imei = ?";
        PreparedStatement pstmtimei = connection.prepareStatement(sqlimei);
        pstmtimei.setLong(1, imei);
        ResultSet rsimei=pstmtimei.executeQuery();
        if(!rsimei.isBeforeFirst()) {
        String sql2 = "insert into device(user_b_id,imei) values(?,?)";
        PreparedStatement stmt = connection.prepareStatement(sql2);
        stmt.setInt(1, userBId);
        stmt.setLong(2, imei);
        stmt.executeUpdate();
        }
        else {
          throw new DuplicatedPKException();
        }
      }
      else {
        throw new ForeignKeyException();
      }
    }
    catch(DuplicatedPKException e) {
      
    }
    catch(ForeignKeyException e) {}
  }

  @Override
  public void setNewParent(int childBId, int parentBId)
      throws ForeignKeyException, NotExistException, SQLException {
    this.connect();
    String sql1 = "select parent_id from user_b where user_id=?";
    PreparedStatement pstmt1 = connection.prepareStatement(sql1);
    pstmt1.setInt(1, childBId);
    ResultSet rs1=pstmt1.executeQuery();
    int parent=rs1.getInt("parent_id");
    if(parent==-1) {
      String sql2 = "select user_id from user_b where user_id=?";
      PreparedStatement pstmt2 = connection.prepareStatement(sql2);
      pstmt2.setInt(1, parentBId);
      ResultSet rs2=pstmt2.executeQuery();
      String parentChildren=rs2.getString("children_ids");
      if(rs2.isBeforeFirst()){
        String sql4 = "update user_b set parent_id=? where user_id=?";
        PreparedStatement pstmt4 = connection.prepareStatement(sql4);
        pstmt4.setInt(1, parentBId);
        pstmt4.setInt(2, childBId);
        pstmt4.executeUpdate();
        String sql3 = "update user_b set children_ids=? where user_id=?";
        PreparedStatement pstmt3 = connection.prepareStatement(sql3);
        pstmt3.setString(1, parentChildren+","+childBId);
        pstmt3.setInt(2, parentBId);
        pstmt3.executeUpdate();
      }
      else {
        throw new NotExistException();
      }
    }
    else {
      throw new ForeignKeyException();
    }
    this.close();
  }
  
  @Override
  public void addUserCDevice(int userCId, long imei) throws ForeignKeyException, NotExistException, SQLException {
    this.connect();
    String sql1 = "select user_id from user_C where user_id=?";
    PreparedStatement pstmt1 = connection.prepareStatement(sql1);
    pstmt1.setInt(1, userCId);
    ResultSet rs1=pstmt1.executeQuery();
    String sql2 = "select imei from device where imei=?";
    PreparedStatement pstmt2 = connection.prepareStatement(sql2);
    pstmt2.setLong(1, imei);
    ResultSet rs2=pstmt2.executeQuery();
    if(rs1.isBeforeFirst()&rs2.isBeforeFirst()) {
      String sql3 = "select devices from user_C where where charindex (+'"+imei+",'+,devices)>0";
      PreparedStatement pstmt3 = connection.prepareStatement(sql3);
      pstmt3.setInt(1, userCId);
      ResultSet rs3=pstmt1.executeQuery();
      String devices=rs1.getString("devices");
      if(!rs3.isBeforeFirst()) {
        String sql = "update user_C set devices=? where user_id=?";
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, devices+","+imei);
        pstmt.setLong(2,imei);
        pstmt.executeUpdate();
      }
      else {
        throw new ForeignKeyException();
      }
    }
    else {
      throw new NotExistException();
    }
    this.close();
  }
 
  @Override
  public void authorizeCDevice(long imei, int toCId) throws ForeignKeyException, NotExistException, SQLException {
    this.connect();
    String sql1 = "select user_id from user_C where user_id=?";
    PreparedStatement pstmt1 = connection.prepareStatement(sql1);
    pstmt1.setInt(1, toCId);
    ResultSet rs1=pstmt1.executeQuery();
    String sql2 = "select imei from device where imei=?";
    PreparedStatement pstmt2 = connection.prepareStatement(sql2);
    pstmt2.setLong(1, imei);
    ResultSet rs2=pstmt2.executeQuery();
    if(rs1.isBeforeFirst()&rs2.isBeforeFirst()) {
      String devices=rs1.getString("devices");
      String authed_device=rs1.getString("authed_device");
      if(devices.indexOf(""+imei)!=-1) {
        String sql = "update user_C set authed_device=? where user_id=?";
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, authed_device+","+imei);
        pstmt.setLong(2,imei);
        pstmt.executeUpdate();
      }
      else {
        throw new ForeignKeyException();
      }
    }
    else {
      throw new NotExistException();
    }
    this.close();
    
  }

  @Override
  public void updateDevice(long imei, String deviceType, String deviceName, String projectId,
      boolean enabled, boolean repayment) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void deleteParentLink(int childBId) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void relocateDevice(long imei, int toBid) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void deleteDevice(long imei) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void deleteAuthorization(long imei, int userCId) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void updateExpireDate(long imei, int userBId, Date expireDate)
      throws NotExistException, TimeException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public void removeCDevice(long imei) throws NotExistException {
    // TODO 自动生成的方法存根
    
  }

  @Override
  public boolean close() {
    try {
      connection.close();
    }catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  
}
