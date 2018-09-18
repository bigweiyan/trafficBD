package com.hitbd.proj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.Pair;

import java.sql.DriverManager;
import java.sql.SQLException;


public class HbaseTest {
    
    String fileName = "/home/hadoop/case.txt" ; //测试用例文件
    int MAX_QUERY = 2 ;  //查询并发个数
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
        
        public QueryThread(String parameter,int queryid,Connection connection) {
            this.parameter = parameter;
            this.queryid = queryid;
            this.connection = connection;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            StringBuilder sb = new StringBuilder();
            sb.append("----------- Case " + queryid + " -------------\n");
            
            ArrayList<Integer> userBIds = new ArrayList<>();
            int queryUser = Integer.valueOf(parameter);
            userBIds.add(queryUser);
            QueryFilter queryFilter = new QueryFilter();
            queryFilter.setAllowTimeRange(new Pair<>(new Date(1533225600000L), new Date(1533484800000L)));
            java.sql.Connection igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

            Date date = new Date();
            AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(igniteConnection, 2469, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
            long igniteQueryTime = new Date().getTime() - date.getTime();
            int queryCount = scanner.queries.size();
            sb.append("Ignite query Time: " + igniteQueryTime + "ms;\n");
            scanner.setConnection(connection);
            date = new Date();
            if (scanner.notFinished()) {
                scanner.next(50);
                sb.append("Hbase response Time: " + (new Date().getTime() - date.getTime()) + "ms;\n");
            }
            while (scanner.notFinished()) {
                scanner.next(50);
            }
            long hbaseQueryTime = new Date().getTime() - date.getTime();
            long totalQueryTime = igniteQueryTime + hbaseQueryTime;
            sb.append("Hbase query Time: " + hbaseQueryTime + "ms;\n");
            sb.append("Total Time: " + totalQueryTime + "ms;\n");
            sb.append("Query imei count: " + scanner.totalImei + "\n");
            sb.append("Create query: " + queryCount + "\n");
            sb.append("Scan alarm: " + scanner.getTotalAlarm() + "\n");
            sb.append("Query imei average time: " + totalQueryTime/scanner.totalImei + "ms;\n");
            System.out.println(sb.toString());
            currentThreads.decrementAndGet();
        }
        
    }
}
