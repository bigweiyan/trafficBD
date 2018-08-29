package user_B;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;

public class userBtableTest {

  @Test
  public void test() throws Exception {
    userBtable ub=new userBtable();
    Connection conn= ub.ign.getConnect();
    user_B a=new user_B(1,4,"1,2,3");
    user_B b=new user_B(2,3,"1,4,5");
    user_B c=new user_B(3,1,"5,6,7");
    //ub.delect();
    //ub.createTable();
    //ub.addData(a, conn);
    //ub.addData(b, conn);
    //ub.addData(c, conn);
    user_B d=new user_B(3,1);
    ub.insert(7, 12);
    ub.changeRelationship(a,b);
    //ub.deleteUserB(8);
    String selectsql = "SELECT user_id,parent_id,children_ids FROM user_B";
    Statement stmt = conn.createStatement();
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
  }

}
