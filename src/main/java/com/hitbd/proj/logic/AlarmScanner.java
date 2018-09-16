package com.hitbd.proj.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

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

public class AlarmScanner {
    // 未来可以优化为生产者-消费者模式
    public Queue<Query> queries;
    private List<Pair<Integer, IAlarm>> alarms;
    private int totalAlarm = 0;
    //private int temp = 0;
    private boolean ready = false;  //管理线程添加
    private boolean finished = false;
    
    public ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MAX_THREAD+1);

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection connection;

    public AlarmScanner() {
        alarms = Collections.synchronizedList(new ArrayList<>());
    }

    public AlarmScanner(Connection connection) {
        this.connection = connection;
        alarms = Collections.synchronizedList(new ArrayList<>());
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
    }
    
    synchronized void putAlarm(List<Pair<Integer, IAlarm>> newAlarms) {
        alarms.addAll(newAlarms);
        this.notify();
    }
    
    synchronized void commitQueryResult(int tid,ArrayList<Pair<Integer,IAlarm>> alarms) {
        //TODO 添加到总的alarms列表后要this.notify()，next函数可能在wait中
    }
    
    public synchronized List<Pair<Integer, IAlarm>> next(int count) {
        if(ready == false) {
            ready = true; //管理线程加入线程池
            pool.submit(new ManageThread());
        }

        //while(temp < count) {
        while(getAlarmCount() < count) {
            if(scanFinished()) {
                finished = true;
                return alarms;
            }
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        synchronized(alarms) {
            List<Pair<Integer, IAlarm>> ret = alarms.subList(0, count);
            alarms = alarms.subList(count, alarms.size());
            AlarmScanner.this.queries.notify();
            //List<Pair<Integer, IAlarm>> ret = new ArrayList<>();
            //temp -= count;
            return ret;
        }
    }
    
    public int getTotalAlarm() {
        return totalAlarm;
    }

    public synchronized void addTotalAlarm(int alarm){
        totalAlarm += alarm;
        //temp += alarm;
    }
    
    
    public int getAlarmCount() {
        return alarms.size();
    }
    
    public boolean scanFinished() {
        if(pool.getPoolSize() == 0)
            return true;
        return false;
    }

    public boolean isFinished() {
        return finished;
    }

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
            AlarmScanner.this.putAlarm(result);
        }
    }
    
    class ManageThread implements Runnable {
        public void run() {
            int count = -1; //计数器，分配线程tid
            if(AlarmScanner.this.queries == null)
                return;
            while(!AlarmScanner.this.queries.isEmpty()) {
                while(AlarmScanner.this.getAlarmCount() > Settings.MAX_CACHE_ALARM ) {
                    try {
                        AlarmScanner.this.queries.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                
                if(AlarmScanner.this.pool.getPoolSize() < Settings.MAX_THREAD + 1 ) {
                    // TODO 线程号0-4的5个线程，当1号线程结束时，线程号为0的线程加入，若后加入的0号线程比先加入的0号线程先结束怎么办？
                    pool.submit(new ScanThread(AlarmScanner.this.queries.poll(),(++count)%Settings.MAX_THREAD));
                }
                
            }
        }
    }

}

