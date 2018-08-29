package device;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;

public class DeviceTableTest {


  @Test
  public void test() throws SQLException {
    DeviceTable device = new DeviceTable();
    Connection conn=device.ign.getConnect();
    //device.createTable();
    //device.insert(1,43432);
    //device.insert(2,43433);
    //device.insert(3,43436);
    device.insert(4,43437);
    String selectsql = "SELECT * FROM device";
    Statement stmt = conn.createStatement();
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
  }

}
