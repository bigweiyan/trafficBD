package com.hitbd.proj.action;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.Settings;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestAlarmCount {
    public void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage TestAlarmCount imei startDate endDate # date like mmdd");
            return;
        }
        List<Long> longs = new ArrayList<>();
        longs.add(Long.parseLong(args[1]));
        try (Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG)){
            Date date = new Date();
            Map<Long, Integer> result = HbaseSearch.getInstance().getAlarmCount(connection, args[2],
                    args[3], longs);
            for (Map.Entry<Long, Integer> pair: result.entrySet()) {
                System.out.println(pair.getKey() + ": " + pair.getValue());
            }
            System.out.println("Use Time:" + (new Date().getTime() - date.getTime()) + " ms");

            date = new Date();
            Map<Long, Map<String, Integer>> result2 = HbaseSearch.getInstance().getAlarmCountByStatus(connection, args[2],
                    args[3], longs);
            for (Map.Entry<Long, Map<String, Integer>> entry : result2.entrySet()) {
                System.out.println(entry.getKey());
                for (Map.Entry<String, Integer> entry1 : entry.getValue().entrySet()) {
                    System.out.println("\t" + entry1.getKey() + " " + entry1.getValue());
                }
            }
            System.out.println("Use Time:" + (new Date().getTime() - date.getTime()) + " ms");
        }catch (IOException|NumberFormatException e){
            e.printStackTrace();
        }
    }
}
