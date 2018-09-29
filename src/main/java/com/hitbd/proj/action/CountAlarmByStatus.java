package com.hitbd.proj.action;

import com.hitbd.proj.Settings;
import com.hitbd.proj.model.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class CountAlarmByStatus {
    static Map<Long, Map<String, List<Pair<String, Integer>>>> map = new HashMap<>();
    public static void spilt(File file) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
        Iterator<CSVRecord> records = parser.iterator();
        records.next();
        while (records.hasNext()) {
            CSVRecord record = records.next();
            long imei;
            String push_time = record.get(8).substring(5,7) + record.get(8).substring(8,10);
            String status = record.get(11);

            // 校验行是否符合规范
            try {
                imei = Long.parseLong(record.get(5));
                Integer.parseInt(push_time);
            }catch (NumberFormatException e){
                continue;
            }
            // 找到imei对应的日期表
            if(!map.containsKey(imei)){
                Map<String, List<Pair<String, Integer>>> dateMap = new HashMap<>(32);
                map.put(imei, dateMap);
            }
            Map<String, List<Pair<String, Integer>>> dateMap = map.get(imei);
            // 找到日期对应的告警表
            if(!dateMap.containsKey(push_time)){
                List<Pair<String, Integer>> statusList = new ArrayList<>();
                dateMap.put(push_time, statusList);
            }
            List<Pair<String, Integer>> statusList = dateMap.get(push_time);
            // 找到status对应的pair
            Pair<String, Integer> pair = null;
            for (Pair<String, Integer> candidate : statusList) {
                if (candidate.getKey().equals(status)) {
                    pair = candidate;
                    break;
                }
            }
            if (pair == null) {
                pair = new Pair<>(status, 1);
                statusList.add(pair);
            }else {
                pair.setValue(pair.getValue() + 1);
            }
        }
        parser.close();
    }

    public static void outputMap(Table table) throws IOException{
        List<Put> putList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int putCount = 0;
        for (Map.Entry<Long, Map<String, List<Pair<String, Integer>>>> entry : map.entrySet()) {
            Long imei = entry.getKey();
            Map<String, List<Pair<String, Integer>>> dateMap = entry.getValue();
            for (Map.Entry<String, List<Pair<String,Integer>>> dateEntry : dateMap.entrySet()) {
                String date = dateEntry.getKey();
                List<Pair<String, Integer>> value = dateEntry.getValue();
                sb.setLength(0);
                for (Pair<String, Integer> pair : value) {
                    sb.append(pair.getKey()).append(':').append(pair.getValue()).append(',');
                }
                sb.setLength(sb.length() - 1);
                Put put = new Put(Bytes.toBytes(imei.toString()));
                put.addColumn(Bytes.toBytes("s"), Bytes.toBytes(date), Bytes.toBytes(sb.toString()));
                putList.add(put);

                if (putList.size() > 10000) {
                    putCount++;
                    System.out.println("Start put " + putCount);
                    table.put(putList);
                    putList.clear();
                }
            }
        }
        if (putList.size() > 0) {
            table.put(putList);
            putList.clear();
        }
    }

    public static void main(String[] args){
        try (Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
            Table table = connection.getTable(TableName.valueOf("alarm_count"))) {
            File src=new File(args[1]);
            if (src.exists()) {
                if (src.isFile()) {
                    spilt(src);
                }else if (src.isDirectory()) {
                    for (File f : src.listFiles()) {
                        System.out.print("start:" + f.getName());
                        spilt(f);
                        System.out.println(" Done!");
                    }
                }
                System.out.println("Imei Count " + map.size());
                if (map.size() > 0) {
                    System.out.println("Output map");
                    outputMap(table);
                    System.out.println("Done!");
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
