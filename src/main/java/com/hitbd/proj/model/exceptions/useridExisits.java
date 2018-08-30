package exceptions;

import java.sql.SQLException;

public class useridExisits extends SQLException {
  public useridExisits(int  user_id){
    super();
    System.out.println(String.format("userB id %d already exists!", user_id));
}
}
