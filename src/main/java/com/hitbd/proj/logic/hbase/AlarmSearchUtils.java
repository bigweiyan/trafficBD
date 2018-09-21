package com.hitbd.proj.logic.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Utils;


public class AlarmSearchUtils {

    /**
     * 根据imei与创建时间与E创建行键rouKey
     *
     */
    public String createRowKey(Long imei, Date createTime, int E){
        String imeiString = String.valueOf(imei);
        StringBuilder sb = new StringBuilder();

        for (int j = 0; j < 17 - imeiString.length(); j++) {
            sb.append(0);
        }
        String rowKey = sb.append(imeiString).append(Utils.getRelativeSecond(createTime)).append(String.valueOf(E)).toString();
        return rowKey;
    }

    public HashMap<Long, Pair<String, String>> createRowkeyForQueryByImei(List<Long> imeis){
        HashMap<Long, Pair<String, String>> imeisRowKey = new HashMap<Long, Pair<String, String>>();
        for(Long element: imeis){
            StringBuilder sb = new StringBuilder();
            String imeiStr = String.valueOf(element);
            for (int j = 0; j < 17 - imeiStr.length(); j++) {
                sb.append(0);
            }
            sb.append(element).append("00000").append("0");
            String startRowKey = sb.toString();

            sb.setLength(0);
            imeiStr = String.valueOf(element);
            for (int j = 0; j < 17 - imeiStr.length(); j++) {
                sb.append(0);
            }
            sb.append(imeiStr).append("fffff").append("9");
            String endRowKey = sb.toString();
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            imeisRowKey.put(element, pair);
        }
        return imeisRowKey;
    }
    
    public static void addToList(ResultScanner results,List<Pair<Integer, IAlarm>> ret,Integer userBId,String tablename) {
        
        long basicTime = Utils.getBasedTime(tablename);
        
        SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(Result r:results) {
            IAlarm alarm = new AlarmImpl();
            String rowKey = Bytes.toString(r.getRow());
            alarm.setRowKey(rowKey);
            alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
            alarm.setCreateTime(new Date(Long.valueOf(rowKey.substring(17,22),16) * 1000L + basicTime));
            alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
            alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
            alarm.setViewed(Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("1"));
            String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
            try {
                CSVParser csvparser = CSVParser.parse(record, CSVFormat.DEFAULT);
                List<CSVRecord> csvrecord = csvparser.getRecords();
                alarm.setAddress(csvrecord.get(0).get(0));
                alarm.setEncId(csvrecord.get(0).get(1));
                alarm.setId(csvrecord.get(0).get(2));
                alarm.setLatitude(Float.valueOf(csvrecord.get(0).get(3)));
                alarm.setLongitude(Float.valueOf(csvrecord.get(0).get(4)));
                alarm.setPushTime(dateformatter.parse(csvrecord.get(0).get(5)));
                alarm.setVelocity(Float.valueOf(csvrecord.get(0).get(6)));
                ret.add(new Pair<>(userBId,alarm));
            } catch (IOException | ParseException e1) {
                e1.printStackTrace();
            }
            }
    }

    public Iterator<Result> scanTable(String tableName, String start, String end, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));  //IOException
        Scan scan = new Scan(start.getBytes(),end.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> ret = scanner.iterator();
        scanner.close();
        table.close();
        return ret;
    }
    public void addToList(Iterator<Result> results, List<IAlarm> ret, long basicTime) throws ParseException {
        while(results.hasNext()) {
            Result r = results.next();
            String rowKey = Bytes.toString(r.getRow());
            IAlarm alarm = new AlarmImpl();
            alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
            Date createDate = new Date(Long.valueOf(rowKey.substring(17,22),16)*1000+basicTime);
            alarm.setCreateTime(createDate);
            alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
            alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
            alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
            String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
            String[] recordArr = record.split(",");
            alarm.setAddress(recordArr[0]);
            alarm.setEncId(recordArr[1]);
            alarm.setId(recordArr[2]);
            alarm.setLatitude(Float.valueOf(recordArr[3]));
            alarm.setLongitude(Float.valueOf(recordArr[4]));
            SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            alarm.setPushTime(dateformatter.parse(recordArr[5]));  //parseException
            alarm.setVelocity(Float.valueOf(recordArr[6]));
            ret.add(alarm);
            }
    }

    public void addToList(Iterator<Result> results, Date startTime, Date endTime, List<IAlarm> ret, long basicTime) throws ParseException {
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
                alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                String[] recordArr = record.split(",");
                alarm.setAddress(recordArr[0]);
                alarm.setEncId(recordArr[1]);
                alarm.setId(recordArr[2]);
                alarm.setLatitude(Float.valueOf(recordArr[3]));
                alarm.setLongitude(Float.valueOf(recordArr[4]));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                alarm.setPushTime(dateformatter.parse(recordArr[5]));  //parseException
                alarm.setVelocity(Float.valueOf(recordArr[6]));
                ret.add(alarm);
            }
        }
    }


}
