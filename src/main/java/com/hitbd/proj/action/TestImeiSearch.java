package com.hitbd.proj.action;

import com.hitbd.proj.HbaseSearch;
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

public class TestImeiSearch {
    private BlockingQueue<Long> query;
    private String logDate = new SimpleDateFormat("dd_hh_mm_ss").format(new Date());
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
        try (Scanner scanner = new Scanner(file)){
            while(scanner.hasNext()) {
                long imei = Long.parseLong(scanner.nextLine());
                query.offer(imei);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
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
    }

    class SearchThread extends Thread{
        @Override
        public void run(){
            try (Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
                 FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + getId() + ".log")){
                int queryNo = 0;
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
                    // start work
                    try (AlarmScanner result = HbaseSearch.getInstance()
                            .queryAlarmByImei(batch, HbaseSearch.NO_SORT, null)){
                        result.setConnection(connection);
                        int queryCount = result.queries.size();
                        if (result.notFinished()) {
                            result.next(200);
                            logWriter.write("Response time: " + (new Date().getTime() - date.getTime()) + " ms\n");
                        }
                        if (Settings.Test.WAIT_UNTIL_FINISH) {
                            while (result.notFinished()) {
                                result.next(200);
                            }
                            logWriter.write("Finish time: " + (new Date().getTime() - date.getTime()) + " ms\n");
                        }
                        long totalTime = new Date().getTime() - date.getTime();
                        logWriter.write("Total time: " + totalTime + " ms\n");
                        logWriter.write("Query created: " + queryCount + "\n");
                        logWriter.write("Alarm scanned: " + result.getTotalAlarm() + "\n");
                        logWriter.write("Time spent per IMEI: " + totalTime / imeiBatch.size() + " ms\n");
                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
