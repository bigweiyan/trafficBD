package com.hitbd.proj.action;

import com.hitbd.proj.Settings;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ImportAlarmCount {
    public void main(String[] args){
        if (args.length < 2) {
            System.out.println("Usage: ImportAlarmCount file");
            return;
        }
        File file = new File(args[1]);
        if (!file.exists()) {
            return;
        }
        if (Settings.HBASE_CONFIG == null) {
            System.out.println("HBase Config not find");
        }
        ;
        if (file.isFile()) {
            List<Put> putList = new ArrayList<>();
            try (Scanner scanner = new Scanner(new FileInputStream(file));
                 Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
                 Table table = connection.getTable(TableName.valueOf("alarm_count"))){
                while (scanner.hasNext()) {
                    String[] line = scanner.nextLine().split(",");
                    if (line.length < 2) continue;
                    long imei;
                    try {
                        imei = Long.parseLong(line[0]);
                    }catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                        continue;
                    }
                    for (int i = 1; i < line.length; i++) {
                        String[] kv = line[i].split(":");
                        int v;
                        String k;
                        try {
                            k = kv[0].substring(5,7) + kv[0].substring(8);
                            v = Integer.parseInt(kv[1]);
                        }catch (Exception e){
                            continue;
                        }
                        Put put = new Put(Bytes.toBytes(Long.toString(imei)));
                        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes(k), Bytes.toBytes(Integer.toString(v)));
                        putList.add(put);
                    }
                    if (putList.size() > 1000) {
                        table.put(putList);
                        putList.clear();
                    }
                }
                if (!putList.isEmpty()) {
                    table.put(putList);
                    putList.clear();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
