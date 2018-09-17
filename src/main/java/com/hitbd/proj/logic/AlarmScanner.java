package com.hitbd.proj.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.hitbd.proj.HbaseSearch;
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
    private final PriorityBlockingQueue<Pair<Integer, IAlarm>> cacheAlarms;
    private int totalAlarm = 0;
    private AtomicInteger currentThreads = new AtomicInteger();
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MAX_THREAD+1);
    private Connection connection;
    // 其上会进行加锁，当不能开始新查询时调度线程进入wait，当某个查询结束时工作线程进行notify
    private ManageThread manageThread;
    private boolean[] queryCompleteMark;
    private int[] queryCompleteCount;
    private boolean finish = false;
    private int resultPrepared = 0;
    private int resultTaken = 0;


    public AlarmScanner(int sortType) {
        if (sortType == HbaseSearch.SORT_BY_CREATE_TIME) {
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> e2.getValue().getCreateTime().compareTo(e1.getValue().getCreateTime()));
        }else if (sortType == HbaseSearch.SORT_BY_IMEI) {
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> Long.compare(e2.getValue().getImei(), e1.getValue().getImei()));
        }else{
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> Integer.compare(e2.getKey(), e1.getKey()));
        }
    }

    public AlarmScanner(int sortType, Connection connection) {
        this.connection = connection;
        if (sortType == HbaseSearch.SORT_BY_CREATE_TIME) {
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> e2.getValue().getCreateTime().compareTo(e1.getValue().getCreateTime()));
        }else if (sortType == HbaseSearch.SORT_BY_IMEI) {
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> Long.compare(e2.getValue().getImei(), e1.getValue().getImei()));
        }else{
            cacheAlarms = new PriorityBlockingQueue<>(1000,
                    (e1, e2) -> Integer.compare(e2.getKey(), e1.getKey()));
        }
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
        queryCompleteMark = new boolean[queries.size()];
        queryCompleteCount = new int[queries.size()];
    }
    
    private void commitQueryResult(int tid, List<Pair<Integer,IAlarm>> alarms) {
        // 同步块保证一次只能有一个线程在addAll
        synchronized (cacheAlarms) {
            try {
                while (cacheAlarms.size() > Settings.MAX_CACHE_ALARM) {
                    cacheAlarms.wait();
                }
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
            cacheAlarms.addAll(alarms); // 等同于for each : offer
        }
        currentThreads.decrementAndGet();
        // 唤醒等待空闲线程的manageThread
        synchronized (manageThread) {
            manageThread.notify();
        }
        // 计算累积共有多少有序结果
        synchronized (this) {
            queryCompleteMark[tid] = true;
            queryCompleteCount[tid] = alarms.size();
            for (int i = 0; i < queryCompleteMark.length; i++) {
                finish = finish && queryCompleteMark[i];
                if (!queryCompleteMark[i]) break;
                resultPrepared += queryCompleteCount[i];
            }
            this.notify();
        }
    }

    public int getTotalAlarm() {
        return totalAlarm;
    }

    private void addTotalAlarm(int alarm){
        totalAlarm += alarm;
    }

    public boolean isFinished() {
        return finish;
    }

    public List<Pair<Integer, IAlarm>> next(int count) {
        if (connection == null || queries == null) {
            throw new RuntimeException("queries and connection should be set before run next()");
        }
        if (manageThread == null) {
            manageThread = new ManageThread(queries.size());
        }
        if (finish) return null;
        // 是否准备足够有序结果
        synchronized (this) {
            while (resultPrepared <= resultTaken + count && !finish){
                try {
                    this.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

            }
        }
        List<Pair<Integer, IAlarm>> result = new ArrayList<>();
        // 从缓存中获取count个元素
        cacheAlarms.drainTo(result, count);
        synchronized (cacheAlarms) {
            cacheAlarms.notifyAll(); // 释放因为空间不足阻塞的正在addAll的线程
        }
        resultTaken += result.size();
        return result;
    }

    /**
     * 工作线程
     */
    class ScanThread extends Thread{
        private Query query;
        private int tid;
        ScanThread(Query query, int tid){
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
        ManageThread(int queries){
            this.queries = queries;
        }

        public void run() {
            int nextid = 0; //下一个分配的线程id
            while(nextid < queries) {
                // 取得本对象的锁。锁的目的是等待空闲线程
                synchronized (this) {
                    while (currentThreads.get() >= Settings.MAX_THREAD) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                pool.submit(new ScanThread(AlarmScanner.this.queries.poll(), nextid));
                nextid++;
                currentThreads.incrementAndGet();
            }
        }
    }

}

