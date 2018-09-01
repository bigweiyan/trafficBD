package com.hitbd.proj;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Utils;

public class HbaseSearch implements IHbaseSearch {
    
    private static Connection connection;
    
    @Override
    public boolean connect() {
        if (connection==null||connection.isClosed()){
            try {
                Configuration config = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean connect(Configuration config) {
        if (connection==null||connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public List<IAlarm> getAlarms(long startImei, long endImei, Date startTime, Date endTime) {
        List<IAlarm> ret = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        Date nowdate = calendar.getTime();
        long milliSecond = nowdate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date date48before = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4 - 48 * 1000L * 60 * 60 * 24); 
        
        if(endTime.after(date48before)) {
            Date mmddstartTime = startTime.before(date48before) == true ? date48before : startTime;
            String endTableName = Utils.getTableName(endTime);
            
            Formatter formatter = new Formatter();
            StringBuilder sb = new StringBuilder();
            String imeistr = String.valueOf(startImei);
            for (int j = 0; j < 17 - imeistr.length(); j++) {
                sb.append(0);
            }
            sb.append(startImei).append("00000").append("0");
            String start = sb.toString();
            
            sb.setLength(0);
            imeistr = String.valueOf(endImei);
            for (int j = 0; j < 17 - imeistr.length(); j++) {
                sb.append(0);
            }
            sb.append(endImei).append("fffff").append("9");
            String end = sb.toString();
            
            while(!Utils.getTableName(mmddstartTime).equals(endTableName)) {
                Iterator<Result> results = scanTable(Utils.getTableName(mmddstartTime),start,end);
                milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                addToList(results,startTime,endTime,ret,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
            }
            
            Iterator<Result> results = scanTable(endTableName,start,end);
            milliSecond = endTime.getTime() - Settings.BASETIME;
            period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
            addToList(results,startTime,endTime,ret,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
            
        }
        
        if(startTime.before(date48before)) {
            //Utils.getTableName(startTime).equals(Utils.getTableName(endTime))
            Date historyendTime = endTime.before(date48before) == true ? endTime : date48before;
            
            //history表中获取 scan范围计算
            Formatter formatter = new Formatter();
            StringBuilder sb = new StringBuilder();
            String imeistr = String.valueOf(startImei);
            for (int j = 0; j < 17 - imeistr.length(); j++) {
                sb.append(0);
            }
            sb.append(startImei).append(formatter.format("%08x", startTime.getTime()/1000).toString()).append("0");
            String start = sb.toString();
            
            sb.setLength(0);
            imeistr = String.valueOf(endImei);
            for (int j = 0; j < 17 - imeistr.length(); j++) {
                sb.append(0);
            }
            sb.append(endImei).append(formatter.format("%08x", historyendTime.getTime()/1000).toString()).append("9");
            String end = sb.toString();
            
            
            Iterator<Result> results = scanTable("alarm_history",start,end);
            while(results.hasNext()) {
                Result r = results.next();
                IAlarm alarm = new AlarmImpl();
                String rowKey = Bytes.toString(r.getRow());
                alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
                alarm.setCreateTime(new Date(Long.valueOf(rowKey.substring(17,25),16)*1000));
                alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
                alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
                alarm.setViewed(Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0")==true?false:true);
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                String[] recordArr = record.split(",");
                alarm.setAddress(recordArr[0]);
                alarm.setEncId(recordArr[1]);
                alarm.setId(recordArr[2]);
                alarm.setLatitude(Float.valueOf(recordArr[3]));
                alarm.setLongitude(Float.valueOf(recordArr[4]));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    alarm.setPushTime(dateformatter.parse(recordArr[5]));
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }  //parseException
                alarm.setVelocity(Float.valueOf(recordArr[6]));
                ret.add(alarm);
            }
            
        }
        
        return ret;
    }
    
    public Iterator<Result> scanTable(String tablename,String start,String end) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan(start.getBytes(),end.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> ret = scanner.iterator();
            scanner.close();
            table.close(); 
            return ret;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }  //IOException
        return null;
    }
    
    public void addToList(Iterator<Result> results, Date startTime, Date endTime, List<IAlarm> ret,long basicTime) {
        while(results.hasNext()) {
            Result r = results.next();
            String rowKey = Bytes.toString(r.getRow());
            
            Date createDate = new Date(Long.valueOf(rowKey.substring(17,22),16)*1000+basicTime);
            
            if(createDate.after(startTime)&&createDate.before(endTime)) {
                IAlarm alarm = new AlarmImpl();
                alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
                alarm.setCreateTime(createDate);
                alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
                alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
                alarm.setViewed(Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0")==true?false:true);
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                String[] recordArr = record.split(",");
                alarm.setAddress(recordArr[0]);
                alarm.setEncId(recordArr[1]);
                alarm.setId(recordArr[2]);
                alarm.setLatitude(Float.valueOf(recordArr[3]));
                alarm.setLongitude(Float.valueOf(recordArr[4]));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    alarm.setPushTime(dateformatter.parse(recordArr[5]));
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }  //parseException
                alarm.setVelocity(Float.valueOf(recordArr[6]));
                ret.add(alarm);
            } 
        }
    }

    @Override
    public void insertAlarm(List<IAlarm> alarms) throws TimeException, ForeignKeyException {
        for(IAlarm alarm:alarms) {
            //异常抛出
            String tablename = alarm.getTableName();
            String rowkey = alarm.getRowKey();
            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tablename));
                Put put = new Put(Bytes.toBytes(rowkey));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                StringBuilder sb = new StringBuilder();
                sb.append('\"').append(alarm.getAddress()).append("\",").append(alarm.getEncId()).append(',').append(alarm.getId()).append(',');
                sb.append(alarm.getLatitude()).append(',').append(alarm.getLongitude()).append(',').append(dateformatter.format(alarm.getPushTime())).append(',').append(alarm.getVelocity());
                put.addColumn("r".getBytes(), "record".getBytes(), sb.toString().getBytes());
                put.addColumn("r".getBytes(), "stat".getBytes(), alarm.getStatus().getBytes());
                put.addColumn("r".getBytes(), "type".getBytes(), alarm.getType().getBytes());
                put.addColumn("r".getBytes(), "viewed".getBytes(), (alarm.isViewed()==true? "1":"0").getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setPushTime(List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException {
        for(Pair<String,String> rowKey:rowKeys) {
            //异常抛出
            String tablename = rowKey.getKey();
            String rowkey = rowKey.getValue();
            
            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tablename));
                
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                byte[] value = result.getValue("r".getBytes(), "record".getBytes());
                String record = Bytes.toString(value);
                String[] tmp = record.split(",");
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                tmp[5] = dateformatter.format(pushTime);
                String newRecord = StringUtils.join(tmp,",");
                
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn("r".getBytes(), "record".getBytes(), newRecord.getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setViewedFlag(List<Pair<String, String>> rowKeys, boolean viewed) throws NotExistException {
        for(Pair<String,String> rowKey:rowKeys) {
            //异常抛出
            String tablename = rowKey.getKey();
            String rowkey = rowKey.getValue();
            
            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tablename));
                
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn("r".getBytes(), "viewed".getBytes(), (viewed==true?"1":"0").getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void deleteAlarm(List<Pair<String, String>> rowKeys) throws NotExistException {
        for(Pair<String,String> rowKey:rowKeys) {
            //异常抛出
            String tablename = rowKey.getKey();
            String rowkey = rowKey.getValue();
            
            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tablename));
                
                Delete delete = new Delete(rowkey.getBytes());
                table.delete(delete);
                table.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<IAlarm> queryAlarmByUser(List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        return null;
    }

    @Override
    public List<IAlarm> queryAlarmByImei(List<Long> imeis, int sortType, QueryFilter filter) {
        return null;
    }

    @Override
    public void asyncQueryAlarmByUser(int qid, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {

    }

    @Override
    public void asyncQueryAlarmByImei(int qid, List<Long> imeis, int sortType, QueryFilter filter) {

    }

    @Override
    public List<IAlarm> queryAlarmByUserC(int userCId, int sortType) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByImeiStatus(int parentBId, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserIdViewed(List<Integer> parentBIds, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserId(List<Integer> parentBIds, boolean recursive, int topK) {
        return null;
    }

    @Override
    public boolean close() {
        if(connection!=null) {
            try {
                connection.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}
