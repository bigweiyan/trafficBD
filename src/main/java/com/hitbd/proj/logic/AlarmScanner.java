package com.hitbd.proj.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.hbase.AlarmSearchUtils;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

/**
 * 对象创建时，开启线程调度器及工作线程
 * 工作线程生产数据，直到查询告警缓存满时，工作线程阻塞
 * 如果一个工作线程结束时，查询没有处理完，则启动另一个工作线程
 */
public class AlarmScanner {
    public Queue<Query> queries;
    // 查询告警缓存 当缓存满导致无法添加时工作线程进入wait, 缓存有空位时主线程进行notify
    private BlockingQueue<Pair<Integer, IAlarm>> cacheAlarms;
    private int totalAlarm = 0;
    private AtomicInteger currentThreads = new AtomicInteger();
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MAX_THREAD+1);
    private Connection connection;
    // 其上会进行加锁，当不能开始新查询时调度线程进入wait，当某个查询结束时工作线程进行notify
    private ManageThread manageThread;
    private boolean[] queryCompleteMark;

    public AlarmScanner(boolean sortByTime) {
        if (sortByTime) {
            cacheAlarms = new PriorityBlockingQueue<>(100,
                    (e1, e2) -> e2.getValue().getCreateTime().compareTo(e1.getValue().getCreateTime()));
        }else {
            cacheAlarms = new LinkedBlockingQueue<>();
        }
    }

    public AlarmScanner(boolean sortByTime, Connection connection) {
        this.connection = connection;
        if (sortByTime) {
            cacheAlarms = new PriorityBlockingQueue<>(100,
                    (e1, e2) -> e2.getValue().getCreateTime().compareTo(e1.getValue().getCreateTime()));
        }else {
            cacheAlarms = new LinkedBlockingQueue<>();
        }
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
        queryCompleteMark = new boolean[queries.size()];
    }
    
    void commitQueryResult(int tid, List<Pair<Integer,IAlarm>> alarms) {
        cacheAlarms.addAll(alarms); // 等同于for each : offer
        currentThreads.decrementAndGet();
        manageThread.notify();
        queryCompleteMark[tid] = true;
    }

    public int getTotalAlarm() {
        return totalAlarm;
    }

    private void addTotalAlarm(int alarm){
        totalAlarm += alarm;
    }

    public boolean isFinished() {
        if (queryCompleteMark == null || currentThreads.get() > 0) return false;
        for (boolean mark : queryCompleteMark) {
            if (!mark) return false;
        }
        return true;
    }

    public synchronized List<Pair<Integer, IAlarm>> next(int count) {
        if (manageThread == null) {
            manageThread = new ManageThread(queries.size());
        }
        return null;
    }

    /**
     * 工作线程
     */
    class ScanThread extends Thread{
        private Query query;
        private int tid;
        public ScanThread(Query query,int tid){
            this.query = query;
            this.tid = tid;  
        }
        @Override
        public void run() {
            
            List<Pair<Integer, IAlarm>> result = new ArrayList<>();
            Table table;
            try {
                table = AlarmScanner.this.connection.getTable(TableName.valueOf(query.tableName));
                String start, end;
                int resultCount = 0;
                for (Pair<Integer, Long> pair: query.imeis) {
                    StringBuilder sb = new StringBuilder();
                    String imei = pair.getValue().toString();
                    for (int j = 0; j < 17 - imei.length(); j++) {
                        sb.append(0);
                    }
                    sb.append(imei).append(query.startRelativeSecond).append("0");
                    start = sb.toString();

                    sb.setLength(0);
                    for (int j = 0; j < 17 - imei.length(); j++) {
                        sb.append(0);
                    }
                    sb.append(imei).append(query.endRelativeSecond).append("9");
                    end = sb.toString();
                    Scan scan = new Scan(start.getBytes(),end.getBytes());
                    scan.addFamily("r".getBytes());
                    scan.setBatch(100);
                    ResultScanner scanner = table.getScanner(scan);
                    AlarmSearchUtils.addToList(scanner, result, pair.getKey(),query.tableName);
                    Result[] results = scanner.next(100);
                    while (results.length != 0) {   // this method never return null
                        resultCount += results.length;
                        results = scanner.next(100);
                    }
                    scanner.close();
                }
                table.close();

                // DEBUG output result size
                AlarmScanner.this.addTotalAlarm(resultCount);
            } catch (IOException e) {
                e.printStackTrace();
            }
            AlarmScanner.this.commitQueryResult(tid, result);
        }
    }

    /**
     * 线程调度器，用于调度工作线程
     */
    class ManageThread implements Runnable {
        int queries = 0;
        public ManageThread(int queries){
            this.queries = queries;
        }

        public void run() {
            int nextid = 0; //下一个分配的线程id
            while(nextid < queries) {
                while(currentThreads.get() >= Settings.MAX_THREAD) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                pool.submit(new ScanThread(AlarmScanner.this.queries.poll(), nextid));
                nextid++;
                currentThreads.incrementAndGet();
            }
        }
    }

}

