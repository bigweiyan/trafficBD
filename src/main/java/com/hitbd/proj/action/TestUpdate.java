package com.hitbd.proj.action;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.exception.ForeignKeyException;
import com.hitbd.proj.exception.NotExistException;
import com.hitbd.proj.exception.TimeException;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

public class TestUpdate {
    
    private Long startTime = 1530374400000L; //2018-07-01 00:00:00
    private Long endTime = 1535731199000L; //2018-08-31 23:59:59
    private Long startImei = 550000000000000L;
    private Long endImei = 560000000000000L;
    private int timeSeed = 1;
    private int imeiSeed = 2;
    
    private Long updateTotalTime = 0L;
    private Long deleteTotalTime = 0L;
    private String logDate = new SimpleDateFormat("dd-HH_mm_ss-").format(new Date());
    
    private int testCount = 1000;
    
    Connection connection;
    
    public void main(String[] args) {
        List<IAlarm> insertList = new ArrayList<>();
        
        
        try {
            connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
            //插入告警
            Random timeRandom = new Random(timeSeed);
            Random imeiRandom = new Random(imeiSeed);
            for(int i=0;i<testCount;i++) {
                System.out.print(".");
                IAlarm alarm = new AlarmImpl();
                alarm.setStatus("offline");
                alarm.setType("other");
                alarm.setViewed(false);
                alarm.setAddress("广西壮族自治区玉林市容县车站西路,联众五金不锈钢配件西北36米");
                alarm.setEncId("579ab92ae4b01ed2065da5c2");
                alarm.setId("7d9e00c98229434fabe9b82ae4358095");
                alarm.setLatitude((float)28.152056);
                alarm.setLongitude((float)112.998809);
                alarm.setPushTime(new Date(startTime));
                alarm.setVelocity((float)0);
                alarm.setCreateTime(new Date(startTime + (long) (timeRandom.nextFloat()*(endTime - startTime + 1))));
                alarm.setImei(startImei + (long) (imeiRandom.nextFloat()*(endImei - startImei + 1)));
                insertList.add(alarm);
                System.out.println(alarm.getImei());
            }
            System.out.println(insertList.size());
            HbaseSearch.getInstance().insertAlarm(connection,insertList);
            //更新及删除告警
            imeiRandom = new Random(imeiSeed);
            for(int i=0;i<testCount;i++) {
                System.out.print(".");
                HashMap<Integer, List<Long>> batch = new HashMap<>();
                List<Long> imeiBatch = new ArrayList<>();
                imeiBatch.add(startImei + (long) (imeiRandom.nextFloat()*(endImei - startImei + 1)));
                batch.put(0, imeiBatch);
                QueryFilter filter = new QueryFilter();
                //filter.setAllowTimeRange(new Pair<>(new Date(startTime), new Date(endTime)));
                AlarmScanner result = HbaseSearch.getInstance()
                        .queryAlarmByImei(connection,batch, HbaseSearch.NO_SORT, filter);
                List<Pair<String, String>> rowKeys = new ArrayList<>();
                while(result.notFinished()) {
                    List<Pair<Integer, IAlarm>> resultList = result.next(100);
                    System.out.println(resultList.size());
                    for(Pair<Integer, IAlarm> alarms : resultList) {
                        rowKeys.add(new Pair<String,String>(alarms.getValue().getTableName(),alarms.getValue().getRowKey()));
                    }
                }
                result.close();
                Date date = new Date();
                HbaseSearch.getInstance().setViewedFlag(connection,rowKeys, true);
                updateTotalTime += (new Date().getTime() - date.getTime());
                date = new Date();
                HbaseSearch.getInstance().deleteAlarm(connection,rowKeys);
                deleteTotalTime += (new Date().getTime() - date.getTime());
                
               
        }
            FileWriter logWriter = new FileWriter(Settings.LOG_DIR + logDate + "main" + ".log");
            logWriter.write(String.format("update average time:%.2fms\n", 1.0f*updateTotalTime/testCount));
            logWriter.write(String.format("delete average time:%.2fms\n", 1.0f*deleteTotalTime/testCount));
            logWriter.close();
            System.out.println("finish");
        
        
            
        }catch (IOException | NotExistException | TimeException | ForeignKeyException e) {
            e.printStackTrace();
        }

    }

}
