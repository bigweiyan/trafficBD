package com.hitbd.proj;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.hitbd.proj.logic.hbase.AlarmSearchUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Utils;
import com.hitbd.proj.util.Serialization;
public class HbaseSearch implements IHbaseSearch {

    private static Connection connection;
    private static Configuration config;

    @Override
    public boolean connect() {
        if (connection==null||connection.isClosed()){
            try {
                config = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
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
                HbaseSearch.config = config;
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
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
            Date mmddstartTime = startTime.before(date48before) ? date48before : startTime;
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
            Date historyendTime = endTime.before(date48before) ? endTime : date48before;

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
                alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
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
                alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
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
                put.addColumn("r".getBytes(), "viewed".getBytes(), (alarm.isViewed()? "1":"0").getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
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
                put.addColumn("r".getBytes(), "viewed".getBytes(), (viewed?"1":"0").getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
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
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<IAlarm> queryAlarmByUser(List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        // TODO 使用imei变量判断是否读取孩子
        // TODO 在没有设置imei过滤的时候读取所有设备
        IgniteSearch  igniteSearchObj = new IgniteSearch();

        // 用户id为user_id的所有子用户
        // HashMap<Integer, ArrayList<Integer>> childrenOfUserB = new HashMap<>();
        HashMap<Integer, List<Long>> directDevicesOfUserB = new HashMap<>();
        HashMap<Integer, ArrayList<Long>> imeiOfDevicesOfUserB = new HashMap<>();

        // 创建使用AlarmSearchUtil的对象,方便操作
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        if(igniteSearchObj.connect()){
            for (Integer userBId : userBIds) {
                if (directDevicesOfUserB.containsKey(userBId)) {
                    directDevicesOfUserB.get(userBId).addAll(utilsObj.getdirectDevicesOfUserB(userBId));
                }else {
                    directDevicesOfUserB.put(userBId, new ArrayList<>(utilsObj.getdirectDevicesOfUserB(userBId)));
                }
                imeiOfDevicesOfUserB.putAll(utilsObj.getImeiOfDevicesOfUserB(userBId));
            }
        }
        // 通过imeiOfDevicesOfUserB 查询用户的所有警告表

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        // 根据imei与创建时间与E创建行键rouKeys
        List<Pair<String, String>> rowKeys = new ArrayList<>();
        Date startTime = allowTimeRange.getKey();
        Date endTime = allowTimeRange.getValue();

        for(Long element: allowIMEIs) {
            String startRowKey = utilsObj.createRowKey(element, startTime, 0);
            String endRowKey = utilsObj.createRowKey(element, endTime, 9);
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            rowKeys.add(pair);
        }

        // 警告表查询结果
        List<IAlarm> queryResult = new ArrayList<>();

        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date date48before = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4 - 48 * 1000L * 60 * 60 * 24);

        if(endTime.after(date48before)) {
            Date mmddstartTime = startTime.before(date48before) ? date48before : startTime;
            String endTableName = Utils.getTableName(endTime);

            while(!Utils.getTableName(mmddstartTime).equals(endTableName)) {
                for(Pair<String, String> pair: rowKeys){
                    try{
                        Iterator<Result> results = utilsObj.scanTable(Utils.getTableName(mmddstartTime),pair.getKey(),pair.getValue(), connection);
                        milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                        period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                        utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                        mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException | ParseException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        if(startTime.before(date48before)){
            for(Pair<String, String> pair: rowKeys){
                try{
                    Iterator<Result> results = utilsObj.scanTable("alarm_history",pair.getKey(),pair.getValue(), connection);
                    utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                }catch (IOException | ParseException e) {
                    e.printStackTrace();
                }
            }
        }

        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>(allowAlarmType);
        for(IAlarm element: queryResult){
            if(!allowUserIdsSet.contains(Integer.parseInt(element.getId())) ||
                    !allowAlarmTypeSet.contains(element.getType())){
                queryResult.remove(element);
            }

        }

        /*
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }
        return queryResult;
    }

    @Override
    public List<IAlarm> queryAlarmByImei(List<Long> imeis, int sortType, QueryFilter filter) {
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        HashMap<Long, Pair<String, String>> rowkeyForQuery = utilsObj.createRowkeyForQueryByImei(imeis);

        // 警告表查询结果
        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));

        List<IAlarm> queryResult = new ArrayList<>();

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        try{
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor[] allTable = admin.listTables();

            for(HTableDescriptor oneTableDescriptor: allTable){
                String oneTableName = oneTableDescriptor.getNameAsString();
                for(Long imei: imeis){
                    try{
                        Pair<String, String> pair = rowkeyForQuery.get(imei);
                        Iterator<Result> results = utilsObj.scanTable(oneTableName,pair.getKey(),pair.getValue(), connection);
                        utilsObj.addToList(results, queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException | ParseException e){
                        e.printStackTrace();
                    }
                }

            }
        }catch (IOException e){
            e.printStackTrace();
        }


        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>(allowAlarmType);
        for(IAlarm element: queryResult){
            if(!allowUserIdsSet.contains(element.getId()) || !allowAlarmTypeSet.contains(element.getType())){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }


        return queryResult;

    }

    @Override
    public void asyncQueryAlarmByUser(int qid, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        IgniteSearch  igniteSearchObj = new IgniteSearch();

        // 用户id为user_id的所有子用户
        // HashMap<Integer, ArrayList<Integer>> childrenOfUserB = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<Integer, List<Long>> directDevicesOfUserB = new HashMap<Integer, List<Long>>();
        HashMap<Integer, ArrayList<Long>> imeiOfDevicesOfUserB = new HashMap<Integer, ArrayList<Long>>();

        // 创建使用AlarmSearchUtil的对象,方便操作
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        if(igniteSearchObj.connect()){
            for (Integer userBId : userBIds) {
                if (directDevicesOfUserB.containsKey(userBId)) {
                    directDevicesOfUserB.get(userBId).addAll(utilsObj.getdirectDevicesOfUserB(userBId));
                }else {
                    directDevicesOfUserB.put(userBId, new ArrayList<>(utilsObj.getdirectDevicesOfUserB(userBId)));
                }
                imeiOfDevicesOfUserB.putAll(utilsObj.getImeiOfDevicesOfUserB(userBId));
            }
        }
        // 通过imeiOfDevicesOfUserB 查询用户的所有警告表

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        // 根据imei与创建时间与E创建行键rouKeys
        List<Pair<String, String>> rowKeys = new ArrayList<>();
        Date startTime = allowTimeRange.getKey();
        Date endTime = allowTimeRange.getValue();

        for(Long element: allowIMEIs) {
            String startRowKey = utilsObj.createRowKey(element, startTime, 0);
            String endRowKey = utilsObj.createRowKey(element, endTime, 9);
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            rowKeys.add(pair);
        }

        // 警告表查询结果
        List<IAlarm> queryResult = new ArrayList<>();

        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date date48before = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4 - 48 * 1000L * 60 * 60 * 24);

        if(endTime.after(date48before)) {
            Date mmddstartTime = startTime.before(date48before) ? date48before : startTime;
            String endTableName = Utils.getTableName(endTime);

            while(!Utils.getTableName(mmddstartTime).equals(endTableName)) {
                for(Pair<String, String> pair: rowKeys){
                    try{
                        Iterator<Result> results = utilsObj.scanTable(Utils.getTableName(mmddstartTime),pair.getKey(),pair.getValue(), connection);
                        milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                        period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                        utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                        mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException | ParseException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        if(startTime.before(date48before)){
            for(Pair<String, String> pair: rowKeys){
                try{
                    Iterator<Result> results = utilsObj.scanTable("alarm_history",pair.getKey(),pair.getValue(), connection);
                    utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                }catch (IOException | ParseException e){
                    e.printStackTrace();
                }
            }
        }

        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>(allowAlarmType);
        for(IAlarm element: queryResult){
            if(!allowUserIdsSet.contains(Integer.parseInt(element.getId()))
                    || !allowAlarmTypeSet.contains(element.getType())){
                queryResult.remove(element);
            }

        }

        /*
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }
        // 将查询结果queryResult 地址可更改
        File f=new File(String.format("Query%d.txt", qid));
        try{
            BufferedWriter bw=new BufferedWriter(new FileWriter(f));
            for(int i=0;i<queryResult.size();i++){
                StringBuffer oneLine = new StringBuffer();
                oneLine.append(queryResult.get(i).getId()).append(" ");
                oneLine.append(queryResult.get(i).getImei()).append(" ");
                oneLine.append(queryResult.get(i).getStatus()).append(" ");
                oneLine.append(queryResult.get(i).getType()).append(" ");
                oneLine.append(queryResult.get(i).getLongitude()).append(" ");
                oneLine.append(queryResult.get(i).getLatitude()).append(" ");
                oneLine.append(queryResult.get(i).getVelocity()).append(" ");
                oneLine.append(queryResult.get(i).getAddress()).append(" ");
                oneLine.append(queryResult.get(i).getCreateTime()).append(" ");
                oneLine.append(queryResult.get(i).getPushTime()).append(" ");
                oneLine.append(queryResult.get(i).isViewed()).append(" ");
                oneLine.append(queryResult.get(i).getEncId());
                bw.write(oneLine.toString());
                bw.newLine();
            }
            bw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void asyncQueryAlarmByImei(int qid, List<Long> imeis, int sortType, QueryFilter filter) {
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        HashMap<Long, Pair<String, String>> rowkeyForQuery = utilsObj.createRowkeyForQueryByImei(imeis);

        // 警告表查询结果
        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));

        List<IAlarm> queryResult = new ArrayList<>();

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        try{
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor[] allTable = admin.listTables();

            for(HTableDescriptor oneTableDescriptor: allTable){
                String oneTableName = oneTableDescriptor.getNameAsString();
                for(Long imei: imeis){
                    try{
                        Pair<String, String> pair = rowkeyForQuery.get(imei);
                        Iterator<Result> results = utilsObj.scanTable(oneTableName,pair.getKey(),pair.getValue(), connection);
                        utilsObj.addToList(results, queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException | ParseException e){
                        e.printStackTrace();
                    }
                }

            }
        }catch (IOException e){
            e.printStackTrace();
        }


        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>(allowAlarmType);
        for(IAlarm element: queryResult){
            if(!allowUserIdsSet.contains(Integer.parseInt(element.getId()))
                    || !allowAlarmTypeSet.contains(element.getType())){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }

        // 将查询结果queryResult 地址可更改
        File f=new File(String.format("Query%d.txt", qid));
        try{
            BufferedWriter bw=new BufferedWriter(new FileWriter(f));
            for(int i=0;i<queryResult.size();i++){
                StringBuffer oneLine = new StringBuffer();
                oneLine.append(queryResult.get(i).getId()).append(" ");
                oneLine.append(queryResult.get(i).getImei()).append(" ");
                oneLine.append(queryResult.get(i).getStatus()).append(" ");
                oneLine.append(queryResult.get(i).getType()).append(" ");
                oneLine.append(queryResult.get(i).getLongitude()).append(" ");
                oneLine.append(queryResult.get(i).getLatitude()).append(" ");
                oneLine.append(queryResult.get(i).getVelocity()).append(" ");
                oneLine.append(queryResult.get(i).getAddress()).append(" ");
                oneLine.append(queryResult.get(i).getCreateTime()).append(" ");
                oneLine.append(queryResult.get(i).getPushTime()).append(" ");
                oneLine.append(queryResult.get(i).isViewed()).append(" ");
                oneLine.append(queryResult.get(i).getEncId());
                bw.write(oneLine.toString());
                bw.newLine();
            }
            bw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public List<IAlarm> queryAlarmByUserC(int userCId, int sortType) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByImeiStatus(int parentBId, boolean recursive) {
    	if(recursive==false) {
    		String sql = "select imei from device where user_id = " + String.valueOf(parentBId);
    		PreparedStatement pstmt = connection.prepareStatement(sql);
    		ResultSet rs = pstmt.executeQuery();
    		Map<String,Integer> map = new HashMap<String,Integer>();
    		ArrayList<Long> imeilist = new ArrayList<Long>();
    		while(rs.next()) {
    			imeilist.add(rs.getLong("imei"));
    		}
    		for(int i = 0;i<imeilist.size();i++) {
    			List<IAlarm> ialarmlist = new ArrayList<IAlarm>();
    			Date endtime = new Date();
        		ialarmlist = getAlarms(imeilist.get(i), imeilist.get(i), new Date(Settings.BASETIME),endtime);
        		String temp = ialarmlist.get(i).getStatus();
        		if(map.containsKey(temp)==true)
        			map.replace(temp, map.get(temp), map.get(temp)+1);
        		else
        			map.put(temp, 1);
    		}
    		return map;
    	}
    	else {
    		
    	}
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserIdViewed(ArrayList<Integer> parentBIds, boolean recursive) {
    	if(recursive==false) {
    		String sql = "select imei,user_b_id from device where user_b_id in ("
    				+ Serialization.listToStr(parentBIds) + ")";
    		Map<String, Integer> map = new HashMap<String, Integer>();
    		Map<Long, Integer> imeimap = new HashMap<Long,Integer>();
    		PreparedStatement pstmt = connection.prepareStatement(sql);
    		ResultSet rs = pstmt.executeQuery();
    		while(rs.next()){
    			imeimap.put(rs.getLong("imei"), rs.getInt("user_b_id"));
    		}
    		IgniteSearch
    		for(int i = 0;i < parentBIds.size();i++) {
    			int count = 0;
    			for(Long imei : imeimap.keySet()) {
    				if(imeimap.get(imei).equals(parentBIds.get(i))) {
    					count = count + 
    				}
    				
    			}
    		}
    	}
    	else {
    		
    	}
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
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}
