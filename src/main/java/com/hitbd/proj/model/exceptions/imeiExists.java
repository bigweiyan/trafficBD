package exceptions;

import java.sql.SQLException;

public class imeiExists extends SQLException {
public imeiExists(Long imei) {
  System.out.println(String.format("the imei    "+ imei+" exists"));
}
}
