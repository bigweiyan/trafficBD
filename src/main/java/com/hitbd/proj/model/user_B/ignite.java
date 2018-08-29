package user_B;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ignite {
//数据库协议地址
  private static String igniteUrl = "jdbc:ignite:thin://127.0.0.1/";
  /**
   * 获取连接
   * @return
   * @throws Exception
   */
 public Connection  getConnect() throws SQLException {
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
}
