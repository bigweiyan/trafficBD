package ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import com.hitbd.proj.IIgniteSearch;
import com.hitbd.proj.Exception.DuplicatedPKException;
import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.IDevice;
import com.hitbd.proj.model.IUserB;
import com.hitbd.proj.model.IUserC;

public class ignite implements IIgniteSearch {
  /**
   * 获取连接
   * @return
   * @throws Exception
   */
 public Connection  getConnect() throws SQLException {
   String igniteUrl = "jdbc:ignite:thin://127.0.0.1/";
   Connection conn = DriverManager.getConnection(igniteUrl);
  return conn;
 }
  /**
   * 关闭连接
   * @param conn
   * @throws SQLException
   * @throws Exception
   */
  public void disConnect(Connection conn) throws SQLException {
    if (conn != null) {
      conn.close();
    }
  }
  @Override
  public boolean connect() throws SQLException {
    String igniteUrl = "jdbc:ignite:thin://127.0.0.1/";
    Connection conn = DriverManager.getConnection(igniteUrl);
    return DriverManager.getConnection(igniteUrl) != null;
  }
  @Override
  public boolean connect(String hostname, int port) {
    return false;
  }
  @Override
  public int getAlarmCount(long imei) {
    // TODO 自动生成的方法存根
    return 0;
  }
  @Override
  public void setAlarmCount(long imei, int count) {
    // TODO 自动生成的方法存根
    
  }
  @Override
  public int getViewedCount(long imei) {
    // TODO 自动生成的方法存根
    return 0;
  }
  @Override
  public void setViewedCount(long imei, int count) {
    // TODO 自动生成的方法存根
    
  }
  @Override
  public IUserB getUserB(int userBId) throws NotExistException {
    // TODO 自动生成的方法存根
    return null;
  }
  @Override
  public IDevice getDevice(int imei) throws NotExistException {
    // TODO 自动生成的方法存根
    return null;
  }
  @Override
  public IUserC getUserC(int userCId) throws NotExistException {
    // TODO 自动生成的方法存根
    return null;
  }
  @Override
  public int createUserB(int parentId) {
    // TODO 自动生成的方法存根
    return 0;
  }
  @Override
  public int createUserC() {
    // TODO 自动生成的方法存根
    return 0;
  }
  @Override
  public void createDevice(long imei, int userBId)
      throws DuplicatedPKException, ForeignKeyException {
    // TODO 自动生成的方法存根
    
  }
  @Override
  public void setNewParent(int childBId, int parentBId)
      throws ForeignKeyException, NotExistException {
    // TODO 自动生成的方法存根
    
  }
  @Override
  public void addUserCDevice(int userCId, long imei) throws ForeignKeyException, NotExistException {
    // TODO 自动生成的方法存根
    
  }
  @Override
  public void authorizeCDevice(long imei, int toCId) throws ForeignKeyException, NotExistException {
    // TODO 自动生成的方法存根
    
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
    // TODO 自动生成的方法存根
    return false;
  }
}
