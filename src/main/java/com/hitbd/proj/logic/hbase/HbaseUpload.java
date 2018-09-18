package com.hitbd.proj.logic.hbase;

import com.hitbd.proj.IgniteSearch;
import com.hitbd.proj.Settings;
import com.hitbd.proj.util.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HbaseUpload {
    private static HashMap<Long, Integer> alarmC;
    private static HashMap<Long, Integer> viewedC;
    private static String[] imeiPrefix = {"", "11", "22", "33", "44"};
    private static int copies = 1;
    public static void main(String args[]) {
        if (args.length < 3) {
            System.out.println("usage: ImportAlarm filename hbaseConfFile");
            return;
        }
        if (args.length >= 4) {
            copies = Integer.valueOf(args[3]);
        }
        Configuration config = HBaseConfiguration.create();
        config.addResource(args[2]);
        HashMap<String, List<Put>> putMap = new HashMap<>();
        alarmC = new HashMap<>();
        viewedC = new HashMap<>();
        try (Connection connection = ConnectionFactory.createConnection(config);
             FileWriter writer = new FileWriter(new File(Settings.logDir, "import" + args[1] + ".log"))){
            System.out.println("Connect Success");
            File src = new File(args[1]);
            if (src.isFile()) {
                uploadFile(src, connection, putMap, writer);
            }else{
                File[] files = src.listFiles();
                for (File file: files) {
                    if (file.isFile())
                        uploadFile(file, connection, putMap, writer);
                    putMap.clear();
                }
            }
        }catch (FileNotFoundException e) {
            System.out.println("File Not found: " + args[1]);
            e.printStackTrace();
        }catch (Exception e2) {
            e2.printStackTrace();
        }

        for (Map.Entry<Long, Integer> count: alarmC.entrySet()) {
            IgniteSearch.getInstance().setAlarmCount(count.getKey(), count.getValue());
        }
        for (Map.Entry<Long, Integer> count: alarmC.entrySet()) {
            IgniteSearch.getInstance().setAlarmCount(count.getKey(), count.getValue());
        }
    }

    public static void uploadFile(File file, Connection connection, HashMap<String, List<Put>> putMap, FileWriter logWriter)
            throws IOException, ParseException {
        int timeRange = (int) ((Settings.END_TIME - Settings.START_TIME) / 1000);
        long totalLength = file.length();
        long lineEstimate = totalLength / 230;
        long lineRead = 0;
        int percentage = 0;
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
        Iterator<CSVRecord> records = parser.iterator();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        records.next();
        Random random = new Random();
        logWriter.write("start " + file.getName() + ": \n");
        Date date = new Date();
        while (records.hasNext()){
            CSVRecord record = records.next();
            // 获取Put列表
            Date createDate;
            try {
                createDate = sdf.parse(record.get(2));
            }catch (ParseException e){
                System.out.println("TimeFormat: " + record.toString());
                continue;
            }
            String tableName = Utils.getTableName(createDate);
            List<Put> putList;
            if (putMap.containsKey(tableName)) {
                putList = putMap.get(tableName);
            }else{
                putList = new ArrayList<>();
                putMap.put(tableName, putList);
            }

            for (int i = 0; i < copies; i++) {
                // 获取RowKey
                String imei = record.get(5);
                try {
                    if (i > 0) imei = imeiPrefix[i] + imei.substring(2);
                }catch (Exception e) {
                    System.out.println("imei " + record.toString() + " at " + i + "th copy");
                    continue;
                }
                long imeiLong;
                try {
                    imeiLong = Long.parseLong(imei);
                }catch (NumberFormatException e) {
                    System.out.println("imei" + record.toString() + " at " + i + "th copy");
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 17 - imei.length(); j++) {
                    sb.append(0);
                }
                sb.append(imei).append(Utils.getRelativeSecond(createDate)).append(random.nextInt(Settings.ROW_KEY_E_FACTOR));
                String rowKey = sb.toString();

                // 获取Record
                sb.setLength(0);
                sb.append('\"').append(record.get(0)).append("\",").append(record.get(3)).append(',').append(record.get(4)).append(',');
                sb.append(record.get(6)).append(',').append(record.get(7)).append(',').append(record.get(8)).append(',').append(record.get(10));
                String rowRecord = sb.toString();

                // 更新AlarmC和ViewedC
                if (alarmC.containsKey(imeiLong)) {
                    alarmC.put(imeiLong, alarmC.get(imeiLong) + 1);
                } else {
                    alarmC.put(imeiLong, 1);
                }

                if (!record.get(9).equals("0")) {
                    if (viewedC.containsKey(imeiLong)) {
                        viewedC.put(imeiLong, viewedC.get(imeiLong) + 1);
                    } else {
                        viewedC.put(imeiLong, 1);
                    }
                }

                // 放入批处理中
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn("r".getBytes(), "record".getBytes(), rowRecord.getBytes());
                put.addColumn("r".getBytes(), "stat".getBytes(), record.get(11).getBytes());
                put.addColumn("r".getBytes(), "type".getBytes(), record.get(1).getBytes());
                put.addColumn("r".getBytes(), "viewed".getBytes(), record.get(9).equals("0") ? "0".getBytes() : "1".getBytes());
                putList.add(put);

                // 判断是否批上传
                if (putList.size() > 10000) {
                    Table table = connection.getTable(TableName.valueOf(tableName));
                    table.put(putList);
                    table.close();
                    putMap.put(tableName, new ArrayList<>());
                }
            }

            // 更新进度
            lineRead++;
            if (lineRead > lineEstimate / 100 * (percentage + 1)) {
                percentage++;
                logWriter.write(".");
                if (percentage % 10 == 0) {
                    logWriter.write(percentage + "%\n");
                }
                logWriter.flush();
            }
        }
        // 上传最后未成批的部分
        for (Map.Entry<String, List<Put>> entry: putMap.entrySet()) {
            String tableName = entry.getKey();
            List<Put> putList = entry.getValue();
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(putList);
            table.close();
        }
        long useTime = (new Date().getTime() - date.getTime()) / 1000;
        logWriter.write("size: " + lineRead + " time used: " + Long.toString(useTime) + "s\n");
        logWriter.write(file.getName() + " finished!\n");
        logWriter.flush();
    }

}
