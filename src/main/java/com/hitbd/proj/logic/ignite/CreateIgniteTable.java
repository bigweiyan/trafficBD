package com.hitbd.proj.logic.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CreateIgniteTable {
    private static final String CREATE_USER_C="CREATE TABLE user_C " +
            "(user_id INTEGER NOT NULL," +
            "devices VARCHAR(50) , "+
            "authed_device VARCHAR(50),"+
            "auth_user_ids VARCHAR(50),"+
            "PRIMARY KEY (user_id))";
    private static final String CREATE_DEVICE="CREATE TABLE device " +
            "(user_b_id INTEGER not NULL," +
            "imei LONG not NULL, "+
            "device_type VARCHAR(50),"+
            "device_name VARCHAR(50),"+
            "project_id VARCHAR(50),"+
            "enabled TINYINT,"+
            "repayment TINYINT,"+
            "expire_list VARCHAR(50),"+
            "user_c_id INTEGER  NULL,"+
            "PRIMARY KEY (imei))";
    private static final String CREATE_USER_B = "CREATE TABLE user_B " +
            "(user_id INTEGER not NULL," +
            "parent_id INTEGER , "+
            "children_ids VARCHAR(4096),"+
            "PRIMARY KEY (user_id))";

    public static void main(String[] args) {
        String hostName = "localhost";
        if(args.length > 1) {
            hostName = args[1];
        }
        try (Connection connection = DriverManager.getConnection("jdbc:ignite:thin://" + hostName)) {
            PreparedStatement statement = connection.prepareStatement(CREATE_USER_B);
            statement.execute();
            statement = connection.prepareStatement(CREATE_DEVICE);
            statement.execute();
            statement = connection.prepareStatement(CREATE_USER_C);
            statement.execute();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
