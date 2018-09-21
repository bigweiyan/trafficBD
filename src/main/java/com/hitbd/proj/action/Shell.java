package com.hitbd.proj.action;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Shell {
    java.sql.Connection ignite;
    Connection connection;
    private static final int QUERY_IMEI = 0;
    private static final int QUERY_USER_ID = 1;
    int queryType = 0;
    List<Integer> userIDs;
    int queryUserId = 2214;
    List<Long> imeis;
    Date startTime;
    Date endTime;
    int resultSize = 5;
    SimpleDateFormat sdf;
    public void main(){
        try {
            ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost");
            connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
        }catch (SQLException|IOException e){
            e.printStackTrace();
            return;
        }
        System.out.println("当前配置");
        System.out.println("最大缓存告警数: " + Settings.MAX_CACHE_ALARM);
        System.out.println("工作线程数: " + Settings.MAX_WORKER_THREAD);
        System.out.println("单次工作任务数: " + Settings.MAX_DEVICES_PER_WORKER);
        userIDs = new ArrayList<>();
        imeis = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        boolean stop = false;
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            startTime = sdf.parse("2018-08-01 00:00:00");
            endTime = sdf.parse("2018-08-14 23:59:59");
        }catch (ParseException e){
            e.printStackTrace();
        }
        while (!stop) {
            System.out.println("当前查询类型：" + (queryType == QUERY_IMEI ? "IMEI维度" :  "用户维度"));
            System.out.print("当前查询对象");
            if (queryType == QUERY_IMEI) {
                for (Long imei : imeis) {
                    System.out.print(imei + " ");
                }
            }else {
                for (Integer user : userIDs) {
                    System.out.print(user + " ");
                }
            }
            System.out.println("\n查询起止时间:" + (startTime == null ? "不限" : sdf.format(startTime)) + "-"
                 + (endTime == null ? "不限" : sdf.format(endTime)));
            System.out.println("单次显示结果数:" + resultSize);
            String cmd = scanner.nextLine();
            try {
                switch (cmd) {
                    case "quit":
                    case "exit":
                        stop = true;
                        break;
                    case "result size":
                        resultSize = Integer.parseInt(scanner.nextLine());
                        break;
                    case "query imei":
                        queryType = QUERY_IMEI;
                        break;
                    case "query user":
                        queryType = QUERY_USER_ID;
                        break;
                    case "add":
                        if (queryType == QUERY_IMEI){
                            imeis.add(Long.parseLong(scanner.nextLine()));
                        }else {
                            userIDs.add(Integer.parseInt(scanner.nextLine()));
                        }
                        break;
                    case "del":
                        if (queryType == QUERY_IMEI) {
                            if (!imeis.isEmpty()) imeis.remove(imeis.size() - 1);
                        }else {
                            if (!imeis.isEmpty()) imeis.remove(imeis.size() - 1);
                        }
                    case "as":
                        queryUserId = Integer.valueOf(scanner.nextLine());
                        break;
                    case "start time":
                        startTime = sdf.parse(scanner.nextLine());
                        break;
                    case "end time":
                        endTime = sdf.parse(scanner.nextLine());
                        break;
                    case "run":
                        if (queryType == QUERY_IMEI && imeis.isEmpty()) {
                            System.out.println("please add imei to query");
                            continue;
                        }
                        if (queryType == QUERY_USER_ID && (userIDs.isEmpty() || queryUserId == 0)) {
                            System.out.println("please add user_id to query");
                            continue;
                        }
                        if (queryType == QUERY_IMEI) {
                            imeiSearch(scanner);
                        }else {
                            userSearch(scanner);
                        }
                        imeis.clear();
                        userIDs.clear();
                        break;
                    default:
                        System.out.println("unknown command: " + cmd);
                }
            }catch (ParseException|NumberFormatException p){
                System.out.println(p.getMessage());
            }
        }
        try {
            connection.close();
            ignite.close();
        }catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void imeiSearch(Scanner scanner) {
        HashMap<Integer, List<Long>> map = new HashMap<>();
        map.put(0, imeis);
        QueryFilter filter = new QueryFilter();
        filter.setAllowTimeRange(new Pair<>(startTime, endTime));
        AlarmScanner result = HbaseSearch.getInstance().queryAlarmByImei(map, HbaseSearch.SORT_BY_CREATE_TIME, filter);
        result.setConnection(connection);
        int no = 1;
        int total = 0;
        Date startTime;
        while (true) {
            if (!result.notFinished()) {
                System.out.println("query finished, " + total + " result found");
                break;
            }
            startTime = new Date();
            List<Pair<Integer, IAlarm>> batch = result.next(resultSize);
            for (Pair<Integer, IAlarm> pair : batch) {
                IAlarm alarm = pair.getValue();
                System.out.println(no +" : imei-"+ alarm.getImei() + " " +
                        sdf.format(alarm.getCreateTime()) + " " + alarm.getAddress());
                no++;
                total++;
            }
            System.out.println(batch.size() + " rows queried, use time " + (new Date().getTime() - startTime.getTime()) + "ms");
            String cmd = scanner.nextLine();
            if (cmd.equals("quit") || cmd.equals("exit")) break;
        }
        result.close();
    }

    private void userSearch(Scanner scanner) {
        QueryFilter filter = new QueryFilter();
        filter.setAllowTimeRange(new Pair<>(startTime, endTime));
        AlarmScanner result = HbaseSearch.getInstance().
                queryAlarmByUser(ignite, queryUserId, userIDs,false, HbaseSearch.SORT_BY_CREATE_TIME, filter);
        result.setConnection(connection);
        int no = 1;
        int total = 0;
        Date startTime;
        while (true) {
            if (!result.notFinished()) {
                System.out.println("query finished, " + total + " result found");
                break;
            }
            startTime = new Date();
            List<Pair<Integer, IAlarm>> batch = result.next(resultSize);
            for (Pair<Integer, IAlarm> pair : batch) {
                IAlarm alarm = pair.getValue();
                System.out.println(no +" : userid-"+ pair.getKey() + " imei-" + alarm.getImei() +
                        " " + sdf.format(alarm.getCreateTime()) + " " + alarm.getAddress());
                no++;
                total++;
            }
            System.out.println(batch.size() + " rows queried, use time " + (new Date().getTime() - startTime.getTime()) + "ms");
            String cmd = scanner.nextLine();
            if (cmd.equals("quit") || cmd.equals("exit")) break;
        }
        result.close();
    }
}
