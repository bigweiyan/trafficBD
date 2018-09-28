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
            List<Pair<Long, Integer>> result = HbaseSearch.getInstance().getAlarmCount(connection, args[2],
                    args[3], longs);
            for (Pair<Long, Integer> pair: result) {
                System.out.println(pair.getKey() + ": " + pair.getValue());
            }
            System.out.println("Use Time:" + (new Date().getTime() - date.getTime()) + " ms");
        }catch (IOException|NumberFormatException e){
            e.printStackTrace();
        }
    }
}
