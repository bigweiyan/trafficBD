package com.hitbd.proj.example;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.IgniteSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.sun.org.apache.bcel.internal.generic.IALOAD;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Example {

    //1.统计某一段时间内超速告警次数
    public void groupByOverSpeed() throws IOException, SQLException{
        // ignite的连接. ignite连接只能在单线程环境中运行，多线程需要开启不同连接
        Connection ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost");
        // HBase的连接. HBase连接可以在多线程环境中运行，无需创建不同HBase对象
        org.apache.hadoop.hbase.client.Connection hbase = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);

        // 输入参数
        int user_id = 546885;
        String startDate = "0801";
        String endDate = "0830";

        // 读一个用户的所有设备
        List<Long> imeis = IgniteSearch.getInstance().getDirectDevices(ignite, user_id, 1, false);
        HashMap<Integer, List<Long>> user = new HashMap<>();
        user.put(331579, imeis);
        // 分别对应每个用户，获取所有的摘要
        int totalAlarm = 0;
        for (Map.Entry<Integer, List<Long>> entry: user.entrySet()) {
            // 找到每个IMEI的摘要
            Map<Long, Map<String, Integer>> results = HbaseSearch.getInstance().getAlarmCountByStatus(hbase, startDate, endDate, entry.getValue());
            // 输出IMEI的摘要信息
            for (Map.Entry<Long, Map<String, Integer>> imeiInfo : results.entrySet()) {
                System.out.println("User: " + entry.getKey() + " imei: " + imeiInfo.getKey() + " overSpeed:"  + imeiInfo.getValue());
                for (Integer i : imeiInfo.getValue().values()) {
                    totalAlarm += i;
                }
            }
        }
        System.out.println("Total Alarm: " + totalAlarm);

        // 关闭ignite连接.
        ignite.close();
    }

    //2.获取超速告警详情
    public void overSpeedDetail() throws SQLException, IOException, ParseException {
        // ignite的连接. ignite连接只能在单线程环境中运行，多线程需要开启不同连接
        Connection ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost");
        // HBase的连接. HBase连接可以在多线程环境中运行，无需创建不同HBase对象
        org.apache.hadoop.hbase.client.Connection hbase = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        List<Integer> user_id = new ArrayList<>();
        List<Long> imeis = new ArrayList<>();
        //输入参数
        Date startTime = sdf.parse("2018-08-03");
        Date endTime = sdf.parse("2018-08-06");
        user_id.add(546885);
        imeis.add(868120198998426L);
        float speed = 0;
        int limit = 100;

        //过滤器设置
        QueryFilter filter = new QueryFilter();
        filter.setAllowTimeRange(new Pair<>(startTime, endTime));
        HashSet<String> stat = new HashSet<>();
        stat.add("6");
        stat.add("overSpeed");
        filter.setAllowAlarmStatus(stat);
        //按user_id查询
        AlarmScanner result = HbaseSearch.getInstance()
                .queryAlarmByUser(hbase,ignite, user_id.get(0), user_id, false, HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);
        //按imei查询
//        HashMap<Integer, List<Long>> batch = new HashMap<>();
//        batch.put(0,imeis);
//        AlarmScanner result = HbaseSearch.getInstance()
//                .queryAlarmByImei(hbase,batch, HbaseSearch.SORT_BY_PUSH_TIME|HbaseSearch.SORT_DESC, filter);

        //对告警进行二次过滤，找出速度大于给定阈值告警，只给出查询前limit条的示例，如何将全部超速告警取出与之类似
        List<Pair<Integer, IAlarm>> ret = new ArrayList<>();
        while (result.notFinished()) {
            List<Pair<Integer, IAlarm>> top = result.next(limit);
            for(Pair<Integer,IAlarm> alarm:top) {
                if(alarm.getValue().getVelocity() > speed)
                    ret.add(alarm);
            }
            if(ret.size() >= limit)
                break;
        }
        //输出超速告警详细信息
        for(Pair<Integer,IAlarm> userAndAlarm:ret) {
            IAlarm alarm = userAndAlarm.getValue();
            System.out.println("id:"+alarm.getId() + " imei:" + alarm.getImei() + " pushtime:" + alarm.getPushTime()
            + " speed:" + alarm.getVelocity() + " ing:" + alarm.getEncId() + " lat:" + alarm.getLatitude() + " addr:" + alarm.getAddress());
        }
    }
}
