package com.hitbd.proj.action;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GenerateImeiCase {
    private int imeiPerFile;
    public void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: GenerateImeiCase FileName/Folder length");
            return;
        }
        File input = new File(args[1]);
        imeiPerFile = Integer.parseInt(args[2]);
        if (!input.exists()) {
            System.out.println("File not found:" + args[1]);
            return;
        }

        Set<Long> set = new HashSet<>();
        if (input.isFile()) {
            addToSet(input, set);
        }else {
            for (File file : input.listFiles()) {
                addToSet(file, set);
            }
        }
        try (FileWriter fileWriter = new FileWriter("imeiCase")){
            for (Long imei: set) {
                fileWriter.write("" + imei + "\n");
            }
            fileWriter.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void addToSet(File file, Set<Long> set){
        try {
            CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT);
            Iterator<CSVRecord> records = parser.iterator();
            records.next();
            int count = 0;
            while (records.hasNext()) {
                CSVRecord record = records.next();
                try {
                    long imei = Long.parseLong(record.get(5));
                    if (!set.contains(imei)) {
                        set.add(imei);
                        count++;
                    }
                }catch (NumberFormatException e){
                    System.out.println("imei " + record);
                }
                if (count >= imeiPerFile) break;
            }
            parser.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
