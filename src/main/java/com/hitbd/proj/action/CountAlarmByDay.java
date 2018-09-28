package com.hitbd.proj.action;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CountAlarmByDay {
    static Map<Long, Map<String, Integer>> map = new HashMap<>();
    public static void spilt(File file) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
        Iterator<CSVRecord> records = parser.iterator();
        records.next();
        while (records.hasNext()) {
            CSVRecord record = records.next();
            long imei;
            try {
                imei = Long.parseLong(record.get(5));
            }catch (NumberFormatException e){
                continue;
            }

            String push_time=record.get(8).substring(0,10);
            if(!map.containsKey(imei)){
                Map<String, Integer> count=new HashMap<>(32);
                count.put(push_time,1);
                map.put(imei,count);
            }
            else{
                Map<String, Integer> count= map.get(imei);
                if(count.containsKey(push_time)){
                    int times=count.get(push_time);
                    count.put(push_time,times+1);
                }
                else{
                    count.put(push_time,1);
                }
                map.put(imei,count);
            }
        }
        parser.close();
    }

    public static void output(PrintWriter out){
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Map<String, Integer>> entry : map.entrySet()) {
            sb.setLength(0);
            sb.append(entry.getKey()).append(',');
            for (Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
                sb.append(entry2.getKey()).append(':').append(entry2.getValue()).append(',');
            }
            sb.setLength(sb.length() - 1);
            sb.append("\n");
            out.print(sb.toString());
        }
        out.flush();
    }

    public static void main(String[] args){
        try (PrintWriter out =new PrintWriter("result.txt")) {
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
                System.out.println("start output");
                output(out);
                System.out.println("Finished!");
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

}
