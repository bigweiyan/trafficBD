package com.hitbd.proj;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HbaseTest {
    
    private String fileName = "/home/hadoop/case.txt" ; //测试用例文件
    private int MAX_QUERY = 2 ;  //查询并发个数
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_QUERY); //指定并发个数
    private AtomicInteger currentThreads = new AtomicInteger();

    @Test
    public void testQuery() throws IOException, SQLException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("/usr/hbase-1.3.2.1/conf/hbase-site.xml");
        Settings.HBASE_CONFIG = configuration;
        Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
        
        int count = 0; //查询个数
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String parameter;  //参数分割
        Date date = new Date();
        while((parameter = in.readLine()) != null) {
            count++;
            while(currentThreads.get() >= MAX_QUERY) { }
            pool.submit(new QueryThread(parameter,count,connection));
            currentThreads.incrementAndGet();
        }
        pool.shutdown();
        boolean finish = false;
        try {
            while(!finish) {
                finish = pool.awaitTermination(20, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Average time :" + (new Date().getTime()-date.getTime())/count + "ms;");

        
    }
    
    class QueryThread implements Runnable {
        String parameter;
        int queryid;
        Connection connection;
        
        public QueryThread(String parameter,int queryid, Connection connection) {
            this.parameter = parameter;
            this.queryid = queryid;
            this.connection = connection;
        }

        @Override
        public void run() {
            StringBuilder sb = new StringBuilder();
            sb.append("----------- Case ").append(queryid).append(" -------------\n");
            
            ArrayList<Integer> userBIds = new ArrayList<>();
            int queryUser = Integer.valueOf(parameter);
            userBIds.add(queryUser);
            QueryFilter queryFilter = new QueryFilter();
            queryFilter.setAllowTimeRange(new Pair<>(new Date(1533225600000L), new Date(1533484800000L)));
            Set<String> type = new HashSet<>();
            queryFilter.setAllowAlarmType(type);
            type.add("other");
            java.sql.Connection igniteConnection = null;
            try {
                igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
            } catch (SQLException e) {
                e.printStackTrace();
            }

            Date date = new Date();
            AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(igniteConnection, queryUser, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
            long igniteQueryTime = new Date().getTime() - date.getTime();
            int queryCount = scanner.queries.size();
            sb.append("Ignite query Time: ").append(igniteQueryTime).append("ms;\n");
            scanner.setConnection(connection);
            date = new Date();
            if (scanner.notFinished()) {
                List<Pair<Integer, IAlarm>> result = scanner.next(50);
                for (Pair<Integer, IAlarm> pair : result) {
                    System.out.println(pair.getValue().getType());
                }
                sb.append("Hbase response Time: ").append(new Date().getTime() - date.getTime()).append("ms;\n");
            }
            while (scanner.notFinished()) {
                scanner.next(50);
            }
            long hbaseQueryTime = new Date().getTime() - date.getTime();
            long totalQueryTime = igniteQueryTime + hbaseQueryTime;
            sb.append("Hbase query Time: ").append(hbaseQueryTime).append("ms;\n");
            sb.append("Total Time: ").append(totalQueryTime).append("ms;\n");
            sb.append("Query imei count: ").append(scanner.totalImei).append("\n");
            sb.append("Create query: ").append(queryCount).append("\n");
            sb.append("Scan alarm: ").append(scanner.getTotalAlarm()).append("\n");
            sb.append("Query imei average time: ").append(totalQueryTime/scanner.totalImei).append("ms;\n");
            System.out.println(sb.toString());
            currentThreads.decrementAndGet();
        }
    }
}
