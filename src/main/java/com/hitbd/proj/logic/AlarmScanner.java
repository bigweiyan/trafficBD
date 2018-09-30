package com.hitbd.proj.logic;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
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
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MAX_WORKER_THREAD + 4);
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
    private boolean queryAdded = false;
    // 剪枝相关变量
    private boolean enablePruning = false;
    private Map<Long, Integer> totalPruningMap = null;
    private Map<Long, Map<String, Integer>> statusPruningMap = null;
    private Map<Long, Map<String, Integer>> readPruningMap = null;
    // TEST
    public int totalImei;

    public AlarmScanner(int sortType) {
        int sortField = sortType & HbaseSearch.FIELD_MASK;
        int sortOrder = sortType & HbaseSearch.ORDER_MASK;
        switch (sortField) {
            case HbaseSearch.SORT_BY_CREATE_TIME:
                if (sortOrder == HbaseSearch.SORT_ASC) {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> e1.getValue().getCreateTime().compareTo(e2.getValue().getCreateTime()));
                }else {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> e2.getValue().getCreateTime().compareTo(e1.getValue().getCreateTime()));
                }
                break;
            case HbaseSearch.SORT_BY_PUSH_TIME:
                if (sortOrder == HbaseSearch.SORT_ASC) {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> e1.getValue().getPushTime().compareTo(e2.getValue().getPushTime()));
                }else {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> e2.getValue().getPushTime().compareTo(e1.getValue().getPushTime()));
                }
                break;
            case HbaseSearch.SORT_BY_IMEI:
                if (sortOrder == HbaseSearch.SORT_ASC) {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> Long.compare(e1.getValue().getImei(), e2.getValue().getImei()));
                }else {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> Long.compare(e2.getValue().getImei(), e1.getValue().getImei()));
                }
                break;
            case HbaseSearch.SORT_BY_USER_ID:
            case HbaseSearch.NO_SORT:
                if (sortOrder == HbaseSearch.SORT_ASC) {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()));
                }else {
                    cacheAlarms = new PriorityBlockingQueue<>(Settings.MAX_CACHE_ALARM,
                            (e1, e2) -> Integer.compare(e2.getKey(), e1.getKey()));
                }
                break;
            default:
                cacheAlarms = new PriorityBlockingQueue<>();
                throw new IllegalArgumentException("undefined sortType");
        }
    }

    public AlarmScanner(int sortType, Connection connection) {
        this(sortType);
        this.connection = connection;

    }

    public void setFilter(QueryFilter filter){
        this.filter = filter;
    }

    public void setConnection(Connection connection) {
        if (this.connection != null) throw new RuntimeException("setConnection should only run once!");
        this.connection = connection;
    }

    public void startPreparePruning(List<Long> imeis, Date start, Date end) {
        new PruningThread(imeis, start, end).start();
    }

    public void setQueries(Queue<Query> queries) {
        if (queryAdded) throw new RuntimeException("setQueries should only run once!");
        queryAdded = true;
        this.queries = queries;
        queryCompleteMark = new boolean[queries.size()];
        queryCompleteCount = new int[queries.size()];
        if (queries == null || queries.size() < 1) finish = true;
    }

    // 线程提交结果
    private void commitQueryResult(int tid, List<Pair<Integer,IAlarm>> alarms) {
        // 同步块保证一次只能有一个线程在addAll
        if (alarms.size() > 0) {
            synchronized (cacheAlarms) {
                try {
                    while (cacheAlarms.size() > Settings.MAX_CACHE_ALARM && tid != nextWaitId) {
                        cacheAlarms.wait();
                        if (closing) return;
                    }
                } catch (InterruptedException e) {
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
        } else {
            queryCompleteMark[tid] = true;
            synchronized (this) {
                finish = true;
                for (int i = 0; i < queryCompleteMark.length; i++) {
                    finish = finish && queryCompleteMark[i];
                    nextWaitId = i;
                    if (!queryCompleteMark[i]) break;
                }
                this.notify();
            }

        }
    }

    public int getTotalAlarm() {
        return resultPrepared;
    }

    public boolean notFinished() {
        return !finish || resultPrepared > resultTaken;
    }

    public List<Pair<Integer, IAlarm>> next(int count) {
        if (connection == null || queries == null) {
            throw new RuntimeException("queries and connection should be set before run next()");
        }
        if (manageThread == null) {
            manageThread = new ManageThread(queries.size());
            pool.execute(manageThread);
        }
        if (finish && resultTaken >= resultPrepared) return null;
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
        if (manageThread != null) {
            synchronized (manageThread) {
                manageThread.notify();
            }
            pool.shutdown();
        }
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
                    // 判断是否进行剪枝，如需要剪枝在本地进行剪枝
                    if (enablePruning) {
                        if (totalPruningMap != null && totalPruningMap.getOrDefault(pair.getValue(), 0) == 0) {
                            continue;
                        }
                        // 如果用户没有对某一列进行筛选，那么这一列的剪枝也没有意义，相当于直接求和也就是上一步的结果。
                        // 因此此时断言pruningMap存在，则allowSet存在
                        if (readPruningMap != null) {
                            int sum = 0;
                            Map<String, Integer> imeiMap = readPruningMap.getOrDefault(pair.getValue(), null);
                            if (imeiMap == null || imeiMap.size() == 0) continue;
                            for (String read: filter.getAllowReadStatus()) {
                                sum += imeiMap.getOrDefault(read, 0);
                            }
                            if (sum == 0) {
                                continue;
                            }
                        }
                        if (statusPruningMap != null) {
                            int sum = 0;
                            Map<String, Integer> imeiMap = statusPruningMap.getOrDefault(pair.getValue(), null);
                            if (imeiMap == null || imeiMap.size() == 0) continue;
                            for (String status : filter.getAllowAlarmStatus()) {
                                sum += imeiMap.getOrDefault(status, 0);
                            }
                            if (sum == 0) continue;
                        }
                    }

                    // 构造查询ROWKEY
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

                    // 构造Scan
                    Scan scan = new Scan(start.getBytes(),end.getBytes());
                    scan.addFamily("r".getBytes());

                    // 设置字段的Filter，其中filterLists是总逻辑，filters是字段的逻辑
                    List<Filter> filterLists = getFilterLists(filter);
                    if (!filterLists.isEmpty()) {
                        FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterLists);
                        scan.setFilter(fList);
                    }

                    // 运行Scan并添加结果
                    ResultScanner scanner = table.getScanner(scan);
                    AlarmSearchUtils.addToList(scanner, result, pair.getKey(),query.tableName);
                    scanner.close();
                }
                table.close();
                if (closing) return;
            } catch (IOException e) {
                e.printStackTrace();
            }
            commitQueryResult(tid, result);
            currentThreads.decrementAndGet();
        }

        private List<Filter> getFilterLists(QueryFilter queryFilter) {
            List<Filter> filterLists = new ArrayList<>();
            if (queryFilter.getAllowAlarmType() != null && queryFilter.getAllowAlarmType().size() != 0){
                List<Filter> filters = new ArrayList<>();
                for (String alarmType : queryFilter.getAllowAlarmType()) {
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(
                            Bytes.toBytes("r"),
                            Bytes.toBytes("type"),
                            CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(alarmType)
                    );
                    filter.setFilterIfMissing(false);
                    filter.setLatestVersionOnly(true);
                    filters.add(filter);
                }
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                filterLists.add(filterList);
            }
            if (queryFilter.getAllowAlarmStatus() != null && queryFilter.getAllowAlarmStatus().size() != 0){
                List<Filter> filters = new ArrayList<>();
                for (String alarmStatus : queryFilter.getAllowAlarmStatus()) {
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(
                            Bytes.toBytes("r"),
                            Bytes.toBytes("stat"),
                            CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(alarmStatus)
                    );
                    filter.setFilterIfMissing(false);
                    filter.setLatestVersionOnly(true);
                    filters.add(filter);
                }
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                filterLists.add(filterList);
            }
            if (queryFilter.getAllowReadStatus() != null && queryFilter.getAllowReadStatus().size() != 0){
                List<Filter> filters = new ArrayList<>();
                for (String readStatus : queryFilter.getAllowReadStatus()) {
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(
                            Bytes.toBytes("r"),
                            Bytes.toBytes("viewed"),
                            CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(readStatus)
                    );
                    filter.setFilterIfMissing(false);
                    filter.setLatestVersionOnly(true);
                    filters.add(filter);
                }
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                filterLists.add(filterList);
            }
            return filterLists;
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
                synchronized (ManageThread.this) {
                    while (currentThreads.get() >= Settings.MAX_WORKER_THREAD) {
                        try {
                            this.wait(50);
                            if (closing) return;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if(!closing) pool.submit(new ScanThread(AlarmScanner.this.queries.poll(), nextid));
                    else return;
                }
                nextid++;
                currentThreads.incrementAndGet();
            }
        }
    }

    /**
     *
     */
    class PruningThread extends Thread {
        List<Long> imeis;
        String startDateInt;
        String endDateInt;
        PruningThread (List<Long> imeis, Date start, Date end) {
            this.imeis = imeis;
            if (start == null) start = new Date(Settings.START_TIME);
            if (end == null) end = new Date(Settings.END_TIME);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(start);
            startDateInt = "" + ((calendar.get(Calendar.MONTH) + 1) * 100 + calendar.get(Calendar.DAY_OF_MONTH));
            calendar.setTime(end);
            endDateInt = "" + ((calendar.get(Calendar.MONTH) + 1) * 100 + calendar.get(Calendar.DAY_OF_MONTH));
        }


        @Override
        public void run() {
            totalPruningMap = HbaseSearch.getInstance().getAlarmCount(connection, startDateInt, endDateInt, imeis);
            if (filter.getAllowReadStatus() != null && filter.getAllowReadStatus().size() != 0) {
                readPruningMap = HbaseSearch.getInstance().getAlarmCountByRead(connection, startDateInt, endDateInt, imeis);
            }
            if (filter.getAllowAlarmStatus() != null && filter.getAllowAlarmStatus().size() != 0) {
                statusPruningMap = HbaseSearch.getInstance().getAlarmCountByStatus(connection, startDateInt, endDateInt, imeis);
            }
            enablePruning = true;
        }
    }

}

