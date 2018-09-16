package com.hitbd.proj.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

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
    private boolean allowput = true;
    private int currentThreadCount = 0;
    private boolean finished = false;

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
    
    public synchronized void addCurrentThreadCount() {
        currentThreadCount++;
    }
    
    public synchronized void minusCurrentThreadCount() {
        currentThreadCount--;
    }
    
    synchronized void putAlarm(List<Pair<Integer, IAlarm>> newAlarms) {
        alarms.addAll(newAlarms);
        this.notify();
    }
    
    public synchronized List<Pair<Integer, IAlarm>> next(int count) {
        int end = Settings.MAX_THREAD-currentThreadCount;
        for(int i=0;i<end;i++) {
            if(queries==null || queries.isEmpty())
                break;
            new ScanThread().start();
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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        synchronized(alarms) {
            List<Pair<Integer, IAlarm>> ret = alarms.subList(0, count);
            alarms = alarms.subList(count, alarms.size());
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
    
    public synchronized Query getQuery() {
        return queries.poll();
    }
    
    public int getAlarmCount() {
        return alarms.size();
    }
    
    public boolean scanFinished() {
        if(currentThreadCount==0 && (queries==null || queries.isEmpty()))
            return true;
        return false;
    }

    public boolean isFinished() {
        return finished;
    }

    class ScanThread extends Thread{
        private Query query;
        //public ScanThread(Query query){
        //    this.query = query;
        //}
        @Override
        public void run() {
            
            AlarmScanner.this.addCurrentThreadCount();
            
            while(true) {
                
                if(AlarmScanner.this.getAlarmCount()>Settings.MAX_CACHE_ALARM) {
                    AlarmScanner.this.minusCurrentThreadCount();
                    return;
                }
                
                query = AlarmScanner.this.getQuery();
                if(query==null) {
                    AlarmScanner.this.minusCurrentThreadCount();
                    synchronized (AlarmScanner.this) {
                        AlarmScanner.this.notify();
                    }
                    return;
                }

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
                        scanner.close();
                    }
                    table.close();

                    // DEBUG output result size
                    AlarmScanner.this.addTotalAlarm(result.size());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                AlarmScanner.this.putAlarm(result);
                
            }
        }
    }

}

