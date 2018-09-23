package com.hitbd.proj.action;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestImeiSearch {
    private BlockingQueue<Long> query;
    private String logDate = new SimpleDateFormat("dd-HH_mm_ss-").format(new Date());
    private AtomicInteger responseTime = new AtomicInteger();
    private AtomicInteger responseCount = new AtomicInteger();
    private AtomicInteger finishedTime = new AtomicInteger();
    private AtomicInteger alarmScanned = new AtomicInteger();
    Connection connection;
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
        query = new LinkedBlockingDeque<>();
        try (Scanner scanner = new Scanner(file);
             FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + "main" + ".log")){
            connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
            while(scanner.hasNext()) {
                long imei = Long.parseLong(scanner.nextLine());
                query.offer(imei);
            }
            int imeis = query.size();


            Date start = new Date();
            logWriter.write("Test start at " + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(start) + "\n");
            System.out.println("Please wait at least "
                    + query.size() / Settings.Test.IMEI_PER_QUERY / Settings.Test.QUERY_THREAD_PER_TEST
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
            logWriter.write(String.format("Average Time:%.2fms\n" , (end.getTime() - start.getTime()) * 1.0f / imeis));
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
            try (FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + getId() + ".log")){
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
                while (!query.isEmpty()) {
                    // prepare input
                    HashMap<Integer, List<Long>> batch = new HashMap<>();
                    List<Long> imeiBatch = new ArrayList<>(Settings.Test.IMEI_PER_QUERY);
                    query.drainTo(imeiBatch, Settings.Test.IMEI_PER_QUERY);
                    if (imeiBatch.size() == 0) break;
                    batch.put(0, imeiBatch);

                    logWriter.write("========Query:" + queryNo + " | Devices: " + imeiBatch.size() + "========\n");
                    queryNo++;
                    Date date = new Date();
                    QueryFilter filter = new QueryFilter();
                    filter.setAllowTimeRange(new Pair<>(new Date(Settings.Test.START_TIME), new Date(Settings.Test.END_TIME)));
                    // start work
                    AlarmScanner result = HbaseSearch.getInstance()
                            .queryAlarmByImei(batch, HbaseSearch.NO_SORT, filter);
                    result.setConnection(connection);
                    int queryCount = result.queries.size();
                    long response = 0;
                    int resultBatchSize = 200;
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
                    logWriter.write("Response time: " + response + " ms\n");
                    logWriter.write("Finish time: " + totalTime + " ms\n");
                    logWriter.write("Query created: " + queryCount + "\n");
                    logWriter.write("Alarm scanned: " + result.getTotalAlarm() + "\n");
                    logWriter.write(String.format("Time used per IMEI: %.2fms\n", (int)totalTime * 1.0f / imeiBatch.size()));
                    result.close();
                    System.out.print(".");
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
