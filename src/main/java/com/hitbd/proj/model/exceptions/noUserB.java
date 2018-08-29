package exceptions;

import java.sql.SQLException;

public class noUserB extends SQLException {
  public noUserB(int id) {
    System.out.println(String.format("new device does not have userB %d", id));
  }
}
