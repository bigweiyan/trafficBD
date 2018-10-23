package com.hitbd.proj.example;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.IgniteSearch;
import com.hitbd.proj.Settings;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Example {
    public void groupByOverSpeed() throws IOException, SQLException{
        // ignite的连接. ignite连接只能在单线程环境中运行，多线程需要开启不同连接
        Connection ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost");
        // HBase的连接. HBase连接可以在多线程环境中运行，无需创建不同HBase对象
        org.apache.hadoop.hbase.client.Connection hbase = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
        // 读一个用户的所有设备
        List<Long> imeis = IgniteSearch.getInstance().getDirectDevices(ignite, 546885, 1, false);
        HashMap<Integer, List<Long>> user = new HashMap<>();
        user.put(331579, imeis);
        // 分别对应每个用户，获取所有的摘要
        int totalAlarm = 0;
        for (Map.Entry<Integer, List<Long>> entry: user.entrySet()) {
            // 找到每个IMEI的摘要
            Map<Long, Map<String, Integer>> results = HbaseSearch.getInstance().getAlarmCountByStatus(hbase, "0801", "0831", entry.getValue());
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
        // 关闭ignite客户端实例. 停止语句需在程序结束前运行,单个测试结束不需要运行这个语句.
        IgniteSearch.getInstance().stop();
    }
}
