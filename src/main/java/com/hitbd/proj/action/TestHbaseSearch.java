package com.hitbd.proj.action;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

public class TestHbaseSearch {
    private BlockingQueue<Long> queryImei;
    private BlockingQueue<Integer> queryUserRecursive;
    private BlockingQueue<Integer> queryUserDirect;
    private String logDate = new SimpleDateFormat("dd-HH_mm_ss-").format(new Date());
    private AtomicInteger responseTime = new AtomicInteger();
    private AtomicInteger responseCount = new AtomicInteger();
    private AtomicInteger finishedTime = new AtomicInteger();
    private AtomicInteger alarmScanned = new AtomicInteger();
    Connection connection;
    
    private int testCount = 500;
    
    public void main(String[] args) {
        // verify input
        String fileName = "conf/imeiCase";
        if (args.length >= 2) {
            fileName = args[1];
        }
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("File not found: " + fileName);
            return;
        }

        if (!file.isFile()) {
            System.out.println("Unsupported file type");
        }
        // accept input
        queryImei = new LinkedBlockingDeque<>();
        queryUserRecursive = new LinkedBlockingDeque<>();
        queryUserDirect = new LinkedBlockingDeque<>();
        try (FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + "main" + ".log")){
            Scanner scanner = new Scanner(file);
            connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
            while(scanner.hasNext() && queryImei.size() < 0.4 * testCount) {
                long imei = Long.parseLong(scanner.nextLine());
                queryImei.offer(imei);
            }
            scanner.close();
            scanner = new Scanner(new File("conf/userCase"));
            while(scanner.hasNext() && queryUserRecursive.size() < 0.04*testCount) {
                int userID = Integer.parseInt(scanner.nextLine());
                queryUserRecursive.offer(userID);
            }
            while(scanner.hasNext() && queryUserDirect.size() < 0.56*testCount) {
                int userID = Integer.parseInt(scanner.nextLine());
                queryUserDirect.offer(userID);
            }
            scanner.close();
            logWriter.write("Imei:"+queryImei.size()+"Recursive:"+queryUserRecursive.size()+"Direct:"+queryUserDirect.size()+"\n");
            Date start = new Date();
            logWriter.write("Test start at " + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(start) + "\n");
            System.out.println("Please wait at least "
                    + testCount / Settings.Test.IMEI_PER_QUERY
                    + " dots");
            // start thread
            SearchThread[] threads = new SearchThread[Settings.Test.QUERY_THREAD_PER_TEST];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new SearchThread();
                threads[i].start();
            }
            try {
                for (Thread thread : threads){
                    thread.join();
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }

            Date end = new Date();
            logWriter.write("Test end at " + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(end) + "\n");
            logWriter.write("Total time:" + (end.getTime() - start.getTime()) + "ms\n");
            logWriter.write(String.format("Average Time:%.2fms\n" , (end.getTime() - start.getTime()) * 1.0f / testCount));
            logWriter.write(String.format("Average response time:%.2fms\n", responseTime.get() * 1.0f / responseCount.get()));
            if (Settings.Test.WAIT_UNTIL_FINISH) {
                logWriter.write(String.format("Average finish time:%dms\n", finishedTime.get() / responseCount.get()));
            }
            logWriter.write("Total alarm scanned:" + alarmScanned.get() + "\n");
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("\nQuery finished");
    }

    class SearchThread extends Thread{
        @Override
        public void run(){
            try (FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + getId() + ".log");
                    java.sql.Connection ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost")){
                int queryNo = 0;
                logWriter.write("wait until finished:" + Settings.Test.WAIT_UNTIL_FINISH + "\n");
                logWriter.write("start time:"
                        + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(Settings.Test.START_TIME))
                        + "\n");
                logWriter.write("end time:"
                        + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(Settings.Test.START_TIME))
                        + "\n");
                logWriter.write("query threads per test:" + Settings.Test.QUERY_THREAD_PER_TEST + "\n");
                logWriter.write("worker threads per query:" + Settings.MAX_WORKER_THREAD + "\n");
                logWriter.write("devices per worker thread:" + Settings.MAX_DEVICES_PER_WORKER + "\n");
                //imei维度查询
                while (!queryImei.isEmpty()) {
                    // prepare input
                    HashMap<Integer, List<Long>> batch = new HashMap<>();
                    List<Long> imeiBatch = new ArrayList<>(Settings.Test.IMEI_PER_QUERY);
                    queryImei.drainTo(imeiBatch, Settings.Test.IMEI_PER_QUERY);
                    if (imeiBatch.size() == 0) break;
                    batch.put(0, imeiBatch);

                    logWriter.write("========Query:" + queryNo + " | Devices: " + imeiBatch.size() + "========\n");
                    queryNo++;
                    
                    Date startTime;
                    Date endTime = new Date(Settings.Test.END_TIME);
                    if(Math.random() < 0.25) {
                        startTime = new Date(Settings.Test.START_TIME_OPTION) ;
                    } else {
                        startTime = new Date(Settings.Test.START_TIME_DEFAULT);
                    }
                    
                    Date date = new Date();
                    QueryFilter filter = new QueryFilter();
                    filter.setAllowTimeRange(new Pair<>(startTime,endTime));
                    HashSet<String> viewed = new HashSet<>();
                    viewed.add("1");
                    filter.setAllowReadStatus(viewed);
                    HashSet<String> status = new HashSet<>();
                    status.add("1");
                    status.add("10");
                    status.add("11");
                    status.add("12");
                    status.add("128");
                    status.add("13");
                    status.add("14");
                    status.add("15");
                    status.add("16");
                    status.add("17");
                    status.add("18");
                    status.add("19");
                    status.add("192");
                    status.add("194");
                    status.add("195");
                    status.add("2");
                    status.add("20");
                    status.add("22");
                    status.add("23");
                    status.add("24");
                    status.add("25");
                    filter.setAllowAlarmStatus(status);
                    // start work
                    AlarmScanner result = HbaseSearch.getInstance()
                            .queryAlarmByImei(connection, batch, HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);
                    int queryCount = result.queries.size();
                    long response = 0;
                    int resultBatchSize = Settings.Test.RESULT_SIZE;
                    int get = Settings.Test.SHOW_ALL_RESULT ? resultBatchSize : 5;
                    if (result.notFinished()) {
                        List<Pair<Integer, IAlarm>> top = result.next(resultBatchSize);
                        if (Settings.Test.SHOW_TOP_RESULT || Settings.Test.SHOW_ALL_RESULT) {
                            for (int i = 0; i < get; i++) {
                                if (i == top.size()) break;
                                IAlarm alarm = top.get(i).getValue();
                                logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                            }
                        }
                        response = new Date().getTime() - date.getTime();
                    }
                    if (Settings.Test.WAIT_UNTIL_FINISH) {
                        while (result.notFinished()) {
                            List<Pair<Integer, IAlarm>> n = result.next(resultBatchSize);
                            if (Settings.Test.SHOW_ALL_RESULT) {
                                for (Pair<Integer, IAlarm> pair : n) {
                                    IAlarm alarm = pair.getValue();
                                    logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                                }
                            }
                        }

                    }
                    long totalTime = new Date().getTime() - date.getTime();
                    responseCount.incrementAndGet();
                    responseTime.addAndGet((int)response);
                    finishedTime.addAndGet((int)totalTime);
                    alarmScanned.addAndGet(result.getTotalAlarm());
                    logWriter.write("Response time: " + response + " ms\n");
                    logWriter.write("Finish time: " + totalTime + " ms\n");
                    logWriter.write("Query created: " + queryCount + "\n");
                    logWriter.write("Alarm scanned: " + result.getTotalAlarm() + "\n");
                    logWriter.write(String.format("Time used per IMEI: %.2fms\n", (int)totalTime * 1.0f / imeiBatch.size()));
                    result.close();
                    System.out.print(".");
                }
                //递归查询
                while (!queryUserRecursive.isEmpty()) {
                    // prepare input
                    List<Integer> userBatch = new ArrayList<>(Settings.Test.USER_PER_QUERY);
                    queryUserRecursive.drainTo(userBatch, Settings.Test.USER_PER_QUERY);
                    if (userBatch.size() == 0) break;

                    logWriter.write("========Query:" + queryNo + " | Users: " + userBatch.size() + "========\n");
                    queryNo++;
                    
                    Date startTime;
                    Date endTime = new Date(Settings.Test.END_TIME);
                    if(Math.random() < 0.25) {
                        startTime = new Date(Settings.Test.START_TIME_OPTION) ;
                    } else {
                        startTime = new Date(Settings.Test.START_TIME_DEFAULT);
                    }
                    
                    Date date = new Date();
                    QueryFilter filter = new QueryFilter();
                    filter.setAllowTimeRange(new Pair<>(startTime, endTime));
                    HashSet<String> viewed = new HashSet<>();
                    viewed.add("1");
                    filter.setAllowReadStatus(viewed);
                    HashSet<String> status = new HashSet<>();
                    status.add("1");
                    status.add("10");
                    status.add("11");
                    status.add("12");
                    status.add("128");
                    status.add("13");
                    status.add("14");
                    status.add("15");
                    status.add("16");
                    status.add("17");
                    status.add("18");
                    status.add("19");
                    status.add("192");
                    status.add("194");
                    status.add("195");
                    status.add("2");
                    status.add("20");
                    status.add("22");
                    status.add("23");
                    status.add("24");
                    status.add("25");
                    filter.setAllowAlarmStatus(status);
                    // start work
                    AlarmScanner result = HbaseSearch.getInstance()
                            .queryAlarmByUser(connection, ignite, userBatch.get(0), userBatch, true, HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);
                    Long igniteTime = new Date().getTime() - date.getTime();
                    int imeiCount = result.totalImei;
                    int queryCount = result.queries.size();
                    long response = 0;
                    int resultBatchSize = Settings.Test.RESULT_SIZE;
                    int get = Settings.Test.SHOW_ALL_RESULT ? resultBatchSize : 5;
                    if (result.notFinished()) {
                        List<Pair<Integer, IAlarm>> top = result.next(resultBatchSize);
                        if (Settings.Test.SHOW_TOP_RESULT || Settings.Test.SHOW_ALL_RESULT) {
                            for (int i = 0; i < get; i++) {
                                IAlarm alarm = top.get(i).getValue();
                                logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                            }
                        }
                        response = new Date().getTime() - date.getTime();
                    }
                    if (Settings.Test.WAIT_UNTIL_FINISH) {
                        while (result.notFinished()) {
                            List<Pair<Integer, IAlarm>> n = result.next(resultBatchSize);
                            if (Settings.Test.SHOW_ALL_RESULT) {
                                for (Pair<Integer, IAlarm> pair : n) {
                                    IAlarm alarm = pair.getValue();
                                    logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                                }
                            }
                        }

                    }
                    long totalTime = new Date().getTime() - date.getTime();
                    responseCount.incrementAndGet();
                    responseTime.addAndGet((int)response);
                    finishedTime.addAndGet((int)totalTime);
                    alarmScanned.addAndGet(result.getTotalAlarm());
                    logWriter.write("Ignite time" + igniteTime + "ms\n");
                    logWriter.write("Response time: " + response + " ms\n");
                    logWriter.write("Finish time: " + totalTime + " ms\n");
                    logWriter.write("Query created: " + queryCount + "\n");
                    logWriter.write("Alarm scanned: " + result.getTotalAlarm() + "\n");
                    if (imeiCount > 0) logWriter.write("Time used per IMEI: " + totalTime / imeiCount + " ms\n");
                    result.close();
                    System.out.print(".");
                }
                
                while (!queryUserDirect.isEmpty()) {
                    // prepare input
                    List<Integer> userBatch = new ArrayList<>(Settings.Test.USER_PER_QUERY);
                    queryUserDirect.drainTo(userBatch, Settings.Test.USER_PER_QUERY);
                    if (userBatch.size() == 0) break;

                    logWriter.write("========Query:" + queryNo + " | Users: " + userBatch.size() + "========\n");
                    queryNo++;
                    
                    Date startTime;
                    Date endTime = new Date(Settings.Test.END_TIME);
                    if(Math.random() < 0.25) {
                        startTime = new Date(Settings.Test.START_TIME_OPTION) ;
                    } else {
                        startTime = new Date(Settings.Test.START_TIME_DEFAULT);
                    }
                    
                    Date date = new Date();
                    QueryFilter filter = new QueryFilter();
                    filter.setAllowTimeRange(new Pair<>(startTime, endTime));
                    HashSet<String> viewed = new HashSet<>();
                    viewed.add("1");
                    filter.setAllowReadStatus(viewed);
                    HashSet<String> status = new HashSet<>();
                    status.add("1");
                    status.add("10");
                    status.add("11");
                    status.add("12");
                    status.add("128");
                    status.add("13");
                    status.add("14");
                    status.add("15");
                    status.add("16");
                    status.add("17");
                    status.add("18");
                    status.add("19");
                    status.add("192");
                    status.add("194");
                    status.add("195");
                    status.add("2");
                    status.add("20");
                    status.add("22");
                    status.add("23");
                    status.add("24");
                    status.add("25");
                    filter.setAllowAlarmStatus(status);
                    // start work
                    AlarmScanner result = HbaseSearch.getInstance()
                            .queryAlarmByUser(connection, ignite, userBatch.get(0), userBatch, false, HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);
                    Long igniteTime = new Date().getTime() - date.getTime();
                    int imeiCount = result.totalImei;
                    int queryCount = result.queries.size();
                    long response = 0;
                    int resultBatchSize = Settings.Test.RESULT_SIZE;
                    int get = Settings.Test.SHOW_ALL_RESULT ? resultBatchSize : 5;
                    if (result.notFinished()) {
                        List<Pair<Integer, IAlarm>> top = result.next(resultBatchSize);
                        if (Settings.Test.SHOW_TOP_RESULT || Settings.Test.SHOW_ALL_RESULT) {
                            for (int i = 0; i < get; i++) {
                                if (i == top.size()) break;
                                IAlarm alarm = top.get(i).getValue();
                                logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                            }
                        }
                        response = new Date().getTime() - date.getTime();
                    }
                    if (Settings.Test.WAIT_UNTIL_FINISH) {
                        while (result.notFinished()) {
                            List<Pair<Integer, IAlarm>> n = result.next(resultBatchSize);
                            if (Settings.Test.SHOW_ALL_RESULT) {
                                for (Pair<Integer, IAlarm> pair : n) {
                                    IAlarm alarm = pair.getValue();
                                    logWriter.write(alarm.getCreateTime() + "," + alarm.getImei() + "," + alarm.getType() + "\n");
                                }
                            }
                        }

                    }
                    long totalTime = new Date().getTime() - date.getTime();
                    responseCount.incrementAndGet();
                    responseTime.addAndGet((int)response);
                    finishedTime.addAndGet((int)totalTime);
                    alarmScanned.addAndGet(result.getTotalAlarm());
                    logWriter.write("Ignite time" + igniteTime + "ms\n");
                    logWriter.write("Response time: " + response + " ms\n");
                    logWriter.write("Finish time: " + totalTime + " ms\n");
                    logWriter.write("Query created: " + queryCount + "\n");
                    logWriter.write("Alarm scanned: " + result.getTotalAlarm() + "\n");
                    if (imeiCount > 0 ) logWriter.write("Time used per IMEI: " + totalTime / imeiCount + " ms\n");
                    result.close();
                    System.out.print(".");
                }
                
            }catch (IOException | SQLException e){
                e.printStackTrace();
            }
        }
    }
}
