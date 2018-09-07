package com.hitbd.proj.logic.ignite;

import com.hitbd.proj.Settings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class DeviceUpload {
    // 记录全局的user-childre关系，避免重复插入
    private static Map<Integer, Set<Integer>> userChildren = new HashMap<>();
    // 记录全局的user-parent关系，避免重复插入
    private static Map<Integer, Integer> userParent = new HashMap<>();
    // 记录全局的imei，避免重复插入
    private static Set<Long> devices = new HashSet<>();
    private static String INSERT_DEVICE = "insert into device" +
            "(user_b_id,imei,device_type,device_name,project_id,enabled,repayment,user_c_id) " +
            "values(?,?,?,?,?,?,?,?)";
    public static void main(String args[]){
        if (args.length < 2) {
            System.out.println("usage: ImportDevice [filename/folder]");
            return;
        }
        File src = new File(args[1]);
        if (src.exists()) {
            try (Connection connection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
                 FileWriter writer = new FileWriter(new File(Settings.logDir, "import" + args[1] + ".log"))){
                System.out.println("Connect Success");
                if (src.isFile()) {
                    uploadFile(connection, src, writer);
                    uploadUser(connection);
                } else {
                    File[] files = src.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file != null && file.isFile()) {
                                uploadFile(connection, file, writer);
                            }
                        }
                    }
                    uploadUser(connection);
                }
            }catch (SQLException | IOException e){
                e.printStackTrace();
            }
        }else {
            System.out.println("File not found");
        }
    }

    private static void uploadUser(Connection connection) throws SQLException{
        // 插入user_b记录
        PreparedStatement pst = connection.prepareStatement("insert into user_b values(?,?,?)");
        Set<Integer> users = new HashSet<>(userParent.keySet());
        users.addAll(userChildren.keySet());
        int userCount = 0;
        for (Integer user: users) {
            insertUserB(pst, user, userParent.get(user), userChildren.get(user));
            userCount++;
            if (userCount % 100 == 0) {
                pst.executeBatch();
            }
        }
        if (userCount % 100 != 0) {
            pst.executeBatch();
        }
    }

    private static void uploadFile(Connection connection, File file, FileWriter logWriter) throws SQLException, IOException {
        logWriter.append("upload " + file.getName());
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
        Iterator<CSVRecord> records = parser.iterator();
        records.next();
        // 插入device并记录user_id
        int deviceCount = 0;
        PreparedStatement pst = connection.prepareStatement(INSERT_DEVICE);
        while (records.hasNext()){
            CSVRecord record = records.next();
            String appid = record.get(0);
            int enabled = record.get(1).equals("Y") ? 1 : 0;
            long imei = Long.parseLong(record.get(2));
            String deviceType = record.get(4);
            int repayment = Integer.parseInt(record.get(5));
            int userID = Integer.parseInt(record.get(6));
            StringBuilder parentIds = new StringBuilder(record.get(7));
            parentIds.setLength(parentIds.length() - 1);
            String[] parents = parentIds.toString().split(",");
            addUser(parents);

            // imei去重：重复的imei不进行插入
            if (!devices.contains(imei)) {
                devices.add(imei);
                insertDevice(pst, userID, imei, deviceType, appid, enabled, repayment);
                deviceCount++;
            }
            // 每100个device进行一次上传
            if (deviceCount > 100) {
                pst.executeBatch();
                deviceCount = 0;
            }
        }
        // 上传剩余的device
        if(deviceCount > 0) {
            pst.executeBatch();
        }
        logWriter.append(" SUCCESS!\n");
        logWriter.flush();
    }

    private static void insertDevice(PreparedStatement pstmt, int userId, long imei, String deviceType, String appid,
                             int enabled, int repayment) throws SQLException{
        pstmt.setInt(1, userId);
        pstmt.setLong(2, imei);
        pstmt.setString(3, deviceType);
        pstmt.setString(4, "undefined");
        pstmt.setString(5, appid);
        pstmt.setBoolean(6, enabled == 1);
        pstmt.setBoolean(7, repayment == 1);
        pstmt.setInt(8, -1);
        pstmt.addBatch();
    }

    private static void insertUserB(PreparedStatement pstmt, int userId, Integer parentId, Set<Integer> children) throws SQLException {
        pstmt.setInt(1, userId);
        if (parentId != null) {
            pstmt.setInt(2, parentId);
        }else {
            pstmt.setNull(2, Types.INTEGER);
        }
        if (children != null) {
            StringBuilder sb = new StringBuilder();
            for (Integer i : children) {
                sb.append(i).append(',');
            }
            sb.setLength(sb.length() - 1);
            pstmt.setString(3, sb.toString());
        }else {
            pstmt.setNull(3, Types.VARCHAR);
        }
        pstmt.addBatch();
    }

    private static void addUser(String[] parentIds){
        if (parentIds == null || parentIds.length == 0) {
            return;
        }
        if (parentIds.length == 1) {
            userParent.put(Integer.valueOf(parentIds[0]), null);
        }
        int userID, parentId;
        for (int j = 1; j < parentIds.length; j++) {
            userID = Integer.parseInt(parentIds[j]);
            parentId = Integer.parseInt(parentIds[j - 1]);
            userParent.put(userID, parentId);
            if (!userChildren.containsKey(parentId)) {
                userChildren.put(parentId, new HashSet<>());
            }
            userChildren.get(parentId).add(userID);
        }
    }
}
