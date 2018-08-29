package exceptions;

import java.sql.SQLException;

public class noParent extends SQLException {
public noParent(int parents_id) {
  System.out.println(String.format("userB parents id %d not exist",parents_id));
}
}
