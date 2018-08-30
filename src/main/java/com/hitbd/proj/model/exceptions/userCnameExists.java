package exceptions;

import java.sql.SQLException;

public class userCnameExists extends SQLException {
  public userCnameExists(int id) {
    System.out.println(String.format("userC id %d exist!", id));
  }
}
