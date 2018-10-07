package com.hitbd.proj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

public class HbaseTest {
    
    private String fileName = "/home/hadoop/case.txt" ; //测试用例文件
    private int MAX_QUERY = 2 ;  //查询并发个数
    private ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_QUERY); //指定并发个数
    private AtomicInteger currentThreads = new AtomicInteger();   
    
    private int testTimes = 10000;
    

    @Ignore
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
            AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(connection, igniteConnection, queryUser,
                    userBIds, true, HbaseSearch.NO_SORT, queryFilter);
            long igniteQueryTime = new Date().getTime() - date.getTime();
            int queryCount = scanner.queries.size();
            sb.append("Ignite query Time: ").append(igniteQueryTime).append("ms;\n");
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

    public void sqlQueryTest() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
        AlarmScanner result = null;
        QueryFilter filter = new QueryFilter();
        HashSet<String> stat = new HashSet<>();
        stat.add("1");stat.add("10");stat.add("11");stat.add("12");stat.add("128");stat.add("13");stat.add("14");stat.add("15");stat.add("16");
        stat.add("17");stat.add("18");stat.add("19");stat.add("192");stat.add("194");stat.add("195");stat.add("2");stat.add("20");stat.add("22");
        stat.add("23");stat.add("24");stat.add("25");stat.add("3");stat.add("32");stat.add("4");stat.add("5");stat.add("6");stat.add("9");
        stat.add("90");stat.add("ACC_OFF");stat.add("ACC_ON");stat.add("in");stat.add("offline");stat.add("out");stat.add("overSpeed");stat.add("riskPointAlarm");stat.add("sensitiveAreasFence");
        stat.add("stayAlert");stat.add("stayTimeIn");stat.add("stayTimeOut");
        HashSet<String> viewed = new HashSet<>();
        viewed.add("0");
        HashSet<String> type = new HashSet<>();
        type.add("other"); //指定告警类型？？
        
        filter.setAllowAlarmStatus(stat);
        filter.setAllowAlarmType(type);
        filter.setAllowReadStatus(viewed);
        
        Calendar calendar = Calendar.getInstance();
        Date endTime = calendar.getTime();
        calendar.add(Calendar.DATE, -3);
        Date threeDaysAgo = calendar.getTime();
        calendar.add(Calendar.DATE, 3);
        calendar.add(Calendar.MONTH, -1);
        Date monthAgo = calendar.getTime();
        
        List<Long> imeis = Arrays.asList(1L,2L); //imei列表
        //1.imei列表length 是否每次都用固定的imei列表
        
        for(int i = 0;i < testTimes;i++) {
            if(Math.random()<0.25)
                filter.setAllowTimeRange(new Pair<>(threeDaysAgo, endTime));
            else
                filter.setAllowTimeRange(new Pair<>(monthAgo, endTime));
            
            if(i<0.3*testTimes) {
                HashMap<Integer, List<Long>> map = new HashMap<>();
                map.put(0, imeis);
                result = HbaseSearch.getInstance().queryAlarmByImei(map,
                        HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);
                result.setConnection(connection);
            }else if(i<0.33*testTimes) {
                
            }
        }
        
    }
}
