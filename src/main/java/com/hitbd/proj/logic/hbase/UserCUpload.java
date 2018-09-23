package com.hitbd.proj.logic.hbase;

import com.hitbd.proj.model.UserC;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UserCUpload {
    private static Map<String,String> count = new HashMap<String,String>();;
    private static void uploadFile(File file) throws IOException {
        Map<String,String> counttemp=new HashMap<String, String>();
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
        Iterator<CSVRecord> records = parser.iterator();
        while (records.hasNext()) {
            CSVRecord record = records.next();
            String imei = record.get(0);
            try {
                Long.parseLong(imei);
            }catch (NumberFormatException e){
                continue;
            }
            String bind_user_id=record.get(1);
            if(!bind_user_id.equals("0") && bind_user_id.length() != 0) {
                if(counttemp.containsKey(bind_user_id)) {
                    String temp = counttemp.get(bind_user_id);
                    temp = temp+","+imei;
                    counttemp.put(bind_user_id, temp);
                }
                else {
                    counttemp.put(bind_user_id, imei);
                }
            }
        }
        count.putAll(counttemp);
        parser.close();
    }

    private static void userCreationAndUpload(Connection connection) throws SQLException {
        for (Map.Entry<String, String> entry : count.entrySet()) {
            try {
                UserC userc=new UserC(Integer.valueOf(entry.getKey()), entry.getValue(), null, null);
                addData(userc,connection);
            }catch (NumberFormatException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void addData(UserC usr,Connection conn) throws SQLException{
        String sql = "insert into user_C values(?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, usr.getUserCId());
        pstmt.setString(2, usr.getDevicesText());
        pstmt.setString(3,usr.getAuthedDevicesText());
        pstmt.setString(4,usr.getAuthUserIdsText());
        pstmt.executeUpdate();
        pstmt.close();
        List<Long> imeis = usr.getDevices();
        for (Long imei : imeis) {
            pstmt = conn.prepareStatement("update device set user_c_id = ? where imei = ?;");
            pstmt.setInt(1, usr.getUserCId());
            pstmt.setLong(2, imei);
            pstmt.executeUpdate();
            pstmt.close();
        }
    }

    public static void main(String[] args){
        if (args.length < 2) {
            System.out.println("Usage ImportUserC file");
        }
        File src = new File(args[1]);
        try {
            Connection connection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
            File[] files = src.listFiles();
            if (!src.isFile()) {
                for (File file : files) {
                    uploadFile(file);
                }
            } else {
                uploadFile(src);
            }
            userCreationAndUpload(connection);
            connection.close();
        }catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
