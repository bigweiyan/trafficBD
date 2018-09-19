package com.hitbd.proj.logic;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.hbase.AlarmSearchUtils;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 对象创建时，开启线程调度器及工作线程
 * 工作线程生产数据，直到查询告警缓存满时，工作线程阻塞
 * 如果一个工作线程结束时，查询没有处理完，则启动另一个工作线程
 */
public class AlarmScanner implements Closeable {
    public Queue<Query> queries;
    // 查询告警缓存 当缓存满导致无法添加时工作线程进入wait, 缓存有空位时主线程进行notify
    private final PriorityBlockingQueue<Pair<Integer, IAlarm>> cacheAlarms;
    // 用于判断是否应该增加线程
    private AtomicInteger currentThreads = new AtomicInteger();
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MAX_QUERY_THREAD + 4);
    private Connection connection;
    // 调度线程。其上会进行加锁，当不能开始新查询时调度线程进入wait，当某个查询结束时工作线程进行notify
    private ManageThread manageThread;
    // 对应查询是否完成，以及对应查询结果数
    private boolean[] queryCompleteMark;
    private int[] queryCompleteCount;
    // 是否完成查询。每次有线程提交结果时更新此变量
    private boolean finish = false;
    // 已产生多少有序结果。每次线程提交结果时更新此变量
    private int resultPrepared = 0;
    // 已获取多少有序结果。每次取结果时更新此变量
    private int resultTaken = 0;
    // 下一个可以运行的id
    private int nextWaitId = 0;
    private QueryFilter filter = null;
    private boolean closing = false;
    // TEST
    public int totalImei;

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

    public void setFilter(QueryFilter filter){
        this.filter = filter;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
        queryCompleteMark = new boolean[queries.size()];
        queryCompleteCount = new int[queries.size()];
    }

    // 线程提交结果
    private void commitQueryResult(int tid, List<Pair<Integer,IAlarm>> alarms) {
        // 同步块保证一次只能有一个线程在addAll
        synchronized (cacheAlarms) {
            try {
                while (cacheAlarms.size() > Settings.MAX_CACHE_ALARM && tid != nextWaitId) {
                    cacheAlarms.wait();
                    if (closing) return;
                }
            }catch (InterruptedException e) {
                e.printStackTrace();
            }

            cacheAlarms.addAll(alarms); // 等同于for each : offer
        }

        // 计算累积共有多少有序结果
        synchronized (this) {
            queryCompleteMark[tid] = true;
            queryCompleteCount[tid] = alarms.size();
            finish = true;
            resultPrepared = 0;
            for (int i = 0; i < queryCompleteMark.length; i++) {
                finish = finish && queryCompleteMark[i];
                nextWaitId = i;
                if (!queryCompleteMark[i]) break;
                resultPrepared += queryCompleteCount[i];
            }
            this.notify();
        }
    }

    public int getTotalAlarm() {
        return resultPrepared;
    }

    public boolean notFinished() {
        return !finish;
    }

    public List<Pair<Integer, IAlarm>> next(int count) {
        if (connection == null || queries == null) {
            throw new RuntimeException("queries and connection should be set before run next()");
        }
        if (manageThread == null) {
            manageThread = new ManageThread(queries.size());
            pool.execute(manageThread);
        }
        if (finish && resultTaken < resultPrepared) return null;
        // 是否准备足够有序结果
        synchronized (this) {
            while (resultPrepared < resultTaken + count && !finish) {
                try {
                    this.wait(50);
                } catch (InterruptedException e) {
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

    @Override
    public void close(){
        closing = true;
        synchronized (cacheAlarms) {
            cacheAlarms.notifyAll();
        }
        synchronized (manageThread) {
            manageThread.notify();
        }
        pool.shutdown();
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
                    // scan.setBatch(100);
                    if (filter.getAllowAlarmType() != null && filter.getAllowAlarmType().size() != 0){
                        // Create a list of filters.
                        List<Filter> filters = new ArrayList<>();

                        // Add specific filter to list.
                        for (String alarmType : filter.getAllowAlarmType()) {
                            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                                    Bytes.toBytes("r"),
                                    Bytes.toBytes("type"),
                                    CompareFilter.CompareOp.EQUAL,
                                    new SubstringComparator(alarmType)
                            );
                            filter.setFilterIfMissing(false);
                            filter.setLatestVersionOnly(false);
                            filters.add(filter);
                        }

                        // Create combined filter.
                        FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                        scan.setFilter(fList);
                    }

                    ResultScanner scanner = table.getScanner(scan);
                    AlarmSearchUtils.addToList(scanner, result, pair.getKey(),query.tableName);
                    scanner.close();
                }
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            AlarmScanner.this.commitQueryResult(tid, result);
            currentThreads.decrementAndGet();
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
                    while (currentThreads.get() >= Settings.MAX_QUERY_THREAD) {
                        try {
                            this.wait(50);
                            if (closing) return;
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

