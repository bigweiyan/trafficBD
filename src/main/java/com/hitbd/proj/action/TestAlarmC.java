package com.hitbd.proj.action;

import com.hitbd.proj.IgniteSearch;

import java.io.FileWriter;
import java.io.IOException;

public class TestAlarmC {
    public void main(String[] args){
        try (FileWriter fileWriter = new FileWriter("testAlarmC.log")){
            long imei = 442304026366966L;
            fileWriter.write(imei +":" + IgniteSearch.getInstance().getAlarmCount(imei));
            fileWriter.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
