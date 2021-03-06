package com.hitbd.proj;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hitbd.proj.exception.ForeignKeyException;
import com.hitbd.proj.exception.NotExistException;
import com.hitbd.proj.exception.TimeException;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.logic.Query;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Serialization;
import com.hitbd.proj.util.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HbaseSearch implements IHbaseSearch {

    private static HbaseSearch search;

    private HbaseSearch(){};
    public static HbaseSearch getInstance() {
        if (search == null) search = new HbaseSearch();
        return search;
    }
/*
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
                ResultScanner results = scanTable(Utils.getTableName(mmddstartTime),start,end);
                milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                addToList(results,startTime,endTime,ret,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
                results.close();
            }

            ResultScanner results = scanTable(endTableName,start,end);
            milliSecond = endTime.getTime() - Settings.BASETIME;
            period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
            addToList(results,startTime,endTime,ret,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);

        }

        if(startTime.before(date48before)) {
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


            ResultScanner results = scanTable("alarm_history",start,end);
            for(Result r:results) {
                IAlarm alarm = new AlarmImpl();
                String rowKey = Bytes.toString(r.getRow());
                alarm.setRowKey(rowKey);
                alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
                alarm.setCreateTime(new Date(Long.valueOf(rowKey.substring(17,25),16)*1000));
                alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
                alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
                alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                try {
                    CSVParser csvparser = CSVParser.parse(record, CSVFormat.DEFAULT);
                    List<CSVRecord> csvrecord = csvparser.getRecords();
                    alarm.setAddress(csvrecord.get(0).get(0));
                    alarm.setEncId(csvrecord.get(0).get(1));
                    alarm.setId(csvrecord.get(0).get(2));
                    alarm.setLatitude(Float.valueOf(csvrecord.get(0).get(3)));
                    alarm.setLongitude(Float.valueOf(csvrecord.get(0).get(4)));
                    SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    alarm.setPushTime(dateformatter.parse(csvrecord.get(0).get(5)));
                    alarm.setVelocity(Float.valueOf(csvrecord.get(0).get(6)));
                    ret.add(alarm);
                } catch (IOException | ParseException e1) {
                    e1.printStackTrace();
                }
            }
            results.close();
        }

        return ret;
    }

    private ResultScanner scanTable(String tableName,String start,String end) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan(start.getBytes(),end.getBytes());
            scan.addFamily("r".getBytes());
            ResultScanner scanner = table.getScanner(scan);
            table.close();
            return scanner;
        } catch (IOException e) {
            e.printStackTrace();
        }  //IOException
        return null;
    }

    private void addToList(ResultScanner results, Date startTime, Date endTime, List<IAlarm> ret,long basicTime) {
        for(Result r:results) {
            String rowKey = Bytes.toString(r.getRow());

            Date createDate = new Date(Long.valueOf(rowKey.substring(17,22),16)*1000+basicTime);

            if(createDate.after(startTime)&&createDate.before(endTime)) {
                IAlarm alarm = new AlarmImpl();
                alarm.setRowKey(rowKey);
                alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
                alarm.setCreateTime(createDate);
                alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
                alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
                alarm.setViewed(!Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0"));
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                try {
                    CSVParser csvparser = CSVParser.parse(record, CSVFormat.DEFAULT);
                    List<CSVRecord> csvrecord = csvparser.getRecords();
                    alarm.setAddress(csvrecord.get(0).get(0));
                    alarm.setEncId(csvrecord.get(0).get(1));
                    alarm.setId(csvrecord.get(0).get(2));
                    alarm.setLatitude(Float.valueOf(csvrecord.get(0).get(3)));
                    alarm.setLongitude(Float.valueOf(csvrecord.get(0).get(4)));
                    SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    alarm.setPushTime(dateformatter.parse(csvrecord.get(0).get(5)));
                    alarm.setVelocity(Float.valueOf(csvrecord.get(0).get(6)));
                    ret.add(alarm);
                } catch (IOException | ParseException e1) {
                    e1.printStackTrace();
                }
            }
        }
        results.close();
    }
*/
    @Override
    public void insertAlarm(Connection connection,List<IAlarm> alarms) throws TimeException, ForeignKeyException {
        for(IAlarm alarm:alarms) {
            //异常抛出
            String tableName = alarm.getTableName();

            StringBuilder sb = new StringBuilder();
            String imeiStr = String.valueOf(alarm.getImei());
            for (int j = 0; j < 17 - imeiStr.length(); j++) {
                sb.append(0);
            }
            sb.append(imeiStr).append(Utils.getRelativeSecond(alarm.getCreateTime())).append((IgniteSearch.getInstance().getAlarmCount(alarm.getImei())+1)%10);
            String rowKey = sb.toString();

            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowKey));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sb = new StringBuilder();
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
    public void setPushTime(Connection connection,List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException {
        for(Pair<String,String> tableRowKey:rowKeys) {
            //异常抛出
            String tableName = tableRowKey.getKey();
            String rowKey = tableRowKey.getValue();

            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tableName));

                Get get = new Get(Bytes.toBytes(rowKey));
                Result result = table.get(get);
                byte[] value = result.getValue("r".getBytes(), "record".getBytes());
                String record = Bytes.toString(value);
                CSVParser csvparser = CSVParser.parse(record, CSVFormat.DEFAULT);
                List<CSVRecord> csvrecord = csvparser.getRecords();
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                StringBuilder sb = new StringBuilder();
                sb.append('\"').append(csvrecord.get(0).get(0)).append("\",").append(csvrecord.get(0).get(1)).append(',').append(csvrecord.get(0).get(2)).append(',');
                sb.append(csvrecord.get(0).get(3)).append(',').append(csvrecord.get(0).get(4)).append(',').append(dateformatter.format(pushTime)).append(',').append(csvrecord.get(0).get(6));

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn("r".getBytes(), "record".getBytes(), sb.toString().getBytes());
                table.put(put);
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setViewedFlag(Connection connection,List<Pair<String, String>> rowKeys, boolean viewed) throws NotExistException {
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
    public void deleteAlarm(Connection connection,List<Pair<String, String>> rowKeys) throws NotExistException {
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
    public AlarmScanner queryAlarmByUser(Connection hbase, java.sql.Connection ignite, int queryUser, List<Integer> userBIds,
                                         boolean recursive, int sortType, QueryFilter filter) {
        if (filter == null) throw new IllegalArgumentException("filter could not be null");
        // 存放用户及其对应设备
        HashMap<Integer, List<Long>> userAndDevice;
        // 读取用户及其对应设备imei,这些设备将被过期时间进行过滤
        if (recursive) {
            userAndDevice = IgniteSearch.getInstance()
                    .getLevelOrderChildrenDevicesOfUserB(ignite, queryUser, false);
        } else {
            userAndDevice = new HashMap<>();
            for (int user : userBIds) userAndDevice
                    .put(user, IgniteSearch.getInstance().getDirectDevices(ignite, user, queryUser, false));
        }
        pruning(hbase, filter, userAndDevice);
        return queryAlarmByImei(hbase, userAndDevice, sortType, filter);
    }

    private void pruning(Connection hbase, QueryFilter filter, HashMap<Integer, List<Long>> userAndDevice){
        Date start = filter.getAllowTimeRange().getKey();
        Date end = filter.getAllowTimeRange().getValue();
        String startDateInt, endDateInt;
        if (start == null) start = new Date(Settings.START_TIME);
        if (end == null) end = new Date(Settings.END_TIME);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        startDateInt = "" + ((calendar.get(Calendar.MONTH) + 1) * 100 + calendar.get(Calendar.DAY_OF_MONTH));
        calendar.setTime(end);
        endDateInt = "" + ((calendar.get(Calendar.MONTH) + 1) * 100 + calendar.get(Calendar.DAY_OF_MONTH));
        List<Long> imeis = new ArrayList<>();
        for (Map.Entry<Integer, List<Long>> entry : userAndDevice.entrySet()) {
            imeis.addAll(entry.getValue());
        }
        Map<Long, Map<String, Integer>> statusPruningMap = null;
        Map<Long, Map<String, Integer>> readPruningMap = null;
        Map<Long, Integer> totalPruningMap = HbaseSearch.getInstance().getAlarmCount(hbase, startDateInt, endDateInt, imeis);
        if (filter.getAllowReadStatus() != null && filter.getAllowReadStatus().size() != 0) {
            readPruningMap = HbaseSearch.getInstance().getAlarmCountByRead(hbase, startDateInt, endDateInt, imeis);
        }
        if (filter.getAllowAlarmStatus() != null && filter.getAllowAlarmStatus().size() != 0) {
            statusPruningMap = HbaseSearch.getInstance().getAlarmCountByStatus(hbase, startDateInt, endDateInt, imeis);
        }

        Set<Long> pruned = new HashSet<>();
        for (Long l : imeis) {
            if (totalPruningMap != null && totalPruningMap.getOrDefault(l, 0) == 0) {
                pruned.add(l);
                continue;
            }
            // 如果用户没有对某一列进行筛选，那么这一列的剪枝也没有意义，相当于直接求和也就是上一步的结果。
            // 因此此时断言pruningMap存在，则allowSet存在
            if (readPruningMap != null) {
                int sum = 0;
                Map<String, Integer> imeiMap = readPruningMap.getOrDefault(l, null);
                if (imeiMap == null || imeiMap.size() == 0) continue;
                for (String read: filter.getAllowReadStatus()) {
                    sum += imeiMap.getOrDefault(read, 0);
                }
                if (sum == 0) {
                    pruned.add(l);
                    continue;
                }
            }
            if (statusPruningMap != null) {
                int sum = 0;
                Map<String, Integer> imeiMap = statusPruningMap.getOrDefault(l, null);
                if (imeiMap == null || imeiMap.size() == 0) continue;
                for (String status : filter.getAllowAlarmStatus()) {
                    sum += imeiMap.getOrDefault(status, 0);
                }
                if (sum == 0) {
                    pruned.add(l);
                }
            }
        }

        if (pruned.size() == 0) return;
        for (Map.Entry<Integer, List<Long>> entry : userAndDevice.entrySet()) {
            List<Long> longs = entry.getValue();
            int len = longs.size();
            for (int i = 0; i < len; i++) {
                if (pruned.contains(longs.get(i))) {
                    longs.remove(i);
                    i--;
                    len--;
                }
            }
        }
    }

    @Override
    public AlarmScanner queryAlarmByImei(Connection hbase,
                                         HashMap<Integer, List<Long>> userAndDevices,
                                         int sortType,
                                         QueryFilter filter) {
        if (filter == null) throw new IllegalArgumentException("filter could not be null");
        AlarmScanner result = new AlarmScanner(sortType);
        result.setFilter(filter);
        result.setConnection(hbase);
        // 如果需要提前剪枝，则此时开始剪枝线程
        if (Settings.ENABLE_PRUNING) {
            List<Long> imeis = new ArrayList<>();
            for (Map.Entry<Integer, List<Long>> entry : userAndDevices.entrySet()) {
                imeis.addAll(entry.getValue());
            }
            result.startPreparePruning(imeis, filter.getAllowTimeRange().getKey(), filter.getAllowTimeRange().getValue());
        }

        // 计算需要在哪些表中进行查询
        List<String> usedTable;
        if (filter.getAllowTimeRange() == null) {
            usedTable = Arrays.asList(Settings.TABLES);
        }else{
            usedTable = Utils.getUseTable(filter.getAllowTimeRange().getKey(), filter.getAllowTimeRange().getValue());
        }

        // TEST output imeis
        int totalImei = 0;
        for (Map.Entry<Integer, List<Long>> imei : userAndDevices.entrySet()) {
            totalImei += imei.getValue().size();
        }
        result.totalImei = totalImei;

        int sortField = sortType & FIELD_MASK;
        int sortOrder = sortType & ORDER_MASK;

        // 划分查询，每个查询按时间进行排列
        LinkedList<Query> queries = new LinkedList<>();
        if (sortField == SORT_BY_CREATE_TIME || sortField == NO_SORT || sortField == SORT_BY_PUSH_TIME) {
            if (sortOrder == SORT_DESC) {
                usedTable.sort(Comparator.reverseOrder());
            }
            for (int i = 0; i < usedTable.size(); i++){
                // 确定这个查询所对应的起止时间
                String startRelativeSecond;
                String endRelativeSecond;
                boolean isFirst = (sortOrder == SORT_ASC && i == 0) || i == usedTable.size() - 1;
                if (isFirst && filter.getAllowTimeRange() != null) {
                    startRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getKey());
                }else {
                    startRelativeSecond = "00000";
                }
                boolean isLast = (sortOrder == SORT_DESC && i == 0) || i == usedTable.size() - 1;
                if (isLast && filter.getAllowTimeRange() != null) {
                    endRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getValue());
                }else {
                    endRelativeSecond = "fffff";
                }
                // 新增查询
                Query query = new Query();
                query.tableName = usedTable.get(i);
                query.startRelativeSecond = startRelativeSecond;
                query.endRelativeSecond = endRelativeSecond;
                List<Pair<Integer, Long>> imeis = new ArrayList<>();
                for (Map.Entry<Integer, List<Long>> user : userAndDevices.entrySet()) {
                    // 记录上次读取的imei位置
                    int lastPostion = 0;
                    // 记录是否读取完这个user的所有imei
                    boolean doneUser = false;
                    while (!doneUser) {
                        doneUser = true;
                        while (lastPostion < user.getValue().size()) {
                            imeis.add(new Pair<>(user.getKey(), user.getValue().get(lastPostion)));
                            lastPostion++;
                            // 如果现在已经读取了一批MAX_DEVICE的imei，则先构建新子查询
                            if (lastPostion % Settings.MAX_DEVICES_PER_WORKER == 0) {
                                doneUser = false;
                                break;
                            }
                        }
                        // 如果一个表中查询的设备数大于100， 则构建一个新查询
                        if (imeis.size() > Settings.MAX_DEVICES_PER_WORKER) {
                            query.imeis = imeis;
                            queries.add(query);
                            query = new Query();
                            query.tableName = usedTable.get(i);
                            query.startRelativeSecond = startRelativeSecond;
                            query.endRelativeSecond = endRelativeSecond;
                            imeis = new ArrayList<>();
                        }
                    }
                }
                query.imeis = imeis;
                queries.add(query);
            }
        }else if (sortField == SORT_BY_IMEI) {
            //对imei进行排序
            List<Pair<Integer,Long>> sortByImei = new ArrayList<>();
            for (Map.Entry<Integer, List<Long>> user : userAndDevices.entrySet()) {
                for(Long imei:user.getValue()) {
                    sortByImei.add(new Pair<>(user.getKey(),imei));
                }
            }
            if (sortOrder == SORT_ASC) {
                sortByImei.sort(Comparator.comparingLong(Pair::getValue));
            }else {
                sortByImei.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
            }

            //对每个imei在每个表中新建子查询
            for(Pair<Integer,Long> imei:sortByImei) {
                for (int i = 0; i < usedTable.size(); i++) {
                 // 确定这个查询所对应的起止时间
                    String startRelativeSecond;
                    String endRelativeSecond;
                    if (i == 0 && filter.getAllowTimeRange() != null) {
                        startRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getKey());
                    }else {
                        startRelativeSecond = "00000";
                    }
                    if (i == usedTable.size() - 1 && filter.getAllowTimeRange() != null) {
                        endRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getValue());
                    }else {
                        endRelativeSecond = "fffff";
                    }
                    // 新增查询
                    Query query = new Query();
                    query.tableName = usedTable.get(i);
                    query.startRelativeSecond = startRelativeSecond;
                    query.endRelativeSecond = endRelativeSecond;
                    List<Pair<Integer, Long>> imeis = new ArrayList<>();
                    imeis.add(imei);
                    query.imeis = imeis;
                    queries.add(query);
                }
            }
        }else if (sortField == SORT_BY_USER_ID) {
            //对userid进行排序
            List<Map.Entry<Integer, List<Long>>> sortByUserId = new ArrayList<>(userAndDevices.entrySet());
            if (sortOrder == SORT_ASC) {
                sortByUserId.sort(Comparator.comparingInt(Map.Entry::getKey));
            }else {
                sortByUserId.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey()));
            }
            //对每个user的所有imei在每个表中构建子查询
            for(Map.Entry<Integer, List<Long>> user :sortByUserId) {
                Query query = new Query();
                List<Pair<Integer, Long>> imeis = new ArrayList<>();
                for (int i = 0; i < usedTable.size(); i++) {
                    // 确定这个查询所对应的起止时间
                    String startRelativeSecond;
                    String endRelativeSecond;
                    if (i == 0 && filter.getAllowTimeRange() != null) {
                        startRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getKey());
                    }else {
                        startRelativeSecond = "00000";
                    }
                    if (i == usedTable.size() - 1 && filter.getAllowTimeRange() != null) {
                        endRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getValue());
                    }else {
                        endRelativeSecond = "fffff";
                    }
                    // 新增查询
                    query.tableName = usedTable.get(i);
                    query.startRelativeSecond = startRelativeSecond;
                    query.endRelativeSecond = endRelativeSecond;
                    // 记录上次读取的imei位置
                    int lastPostion = 0;
                    // 记录是否读取完这个user的所有imei
                    boolean doneUser = false;
                    while (!doneUser) {
                        doneUser = true;
                        while (lastPostion < user.getValue().size()) {
                            imeis.add(new Pair<>(user.getKey(), user.getValue().get(lastPostion)));
                            lastPostion++;
                            // 如果现在已经读取了一批MAX_DEVICE的imei，则先构建新子查询
                            if (lastPostion % Settings.MAX_DEVICES_PER_WORKER == 0) {
                                doneUser = false;
                                break;
                            }
                        }
                        // 如果一个表中查询的设备数大于100， 则构建一个新查询
                        if (imeis.size() > Settings.MAX_DEVICES_PER_WORKER) {
                            query.imeis = imeis;
                            queries.add(query);
                            query = new Query();
                            query.tableName = usedTable.get(i);
                            query.startRelativeSecond = startRelativeSecond;
                            query.endRelativeSecond = endRelativeSecond;
                            imeis = new ArrayList<>();
                        }
                    }
                }
                query.imeis = imeis;
                queries.add(query);
            }
        }else {
            throw new IllegalArgumentException("sort type should be defined in IHbaseSearch");
        }
        result.setQueries(queries);
        return result;
    }

    @Override
    public Map<Long, Integer> getAlarmCount(Connection connection, String start, String end, List<Long> imeis) {
        int startInt, endInt;
        try {
            endInt = Integer.parseInt(end);
            startInt = Integer.parseInt(start);
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("start, end should like mmdd");
        }
        Map<Long, Integer> imeiMap = new HashMap<>();
        try (Table table = connection.getTable(TableName.valueOf("alarm_count"))){
            List<Get> getList = new ArrayList<>();
            for (Long imei : imeis) {
                Get get = new Get(Bytes.toBytes(Long.toString(imei)));
                get.addFamily(Bytes.toBytes("a"));
                getList.add(get);
            }
            Result[] results = table.get(getList);

            for (Result result : results) {
                List<Cell> cells = result.listCells();
                if (cells == null) continue;
                for (Cell cell : cells) {
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String date = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String count = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    try {
                        long imei = Long.parseLong(row);
                        int dateInt = Integer.parseInt(date);
                        if (Utils.dateBetween(startInt, dateInt, endInt)){
                            imeiMap.put(imei, imeiMap.getOrDefault(imei, 0) + Integer.valueOf(count));
                        }
                    } catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }

        return imeiMap;
    }

    public Map<Long, Map<String, Integer>> getAlarmCountByStatus(Connection connection, String start,
                                                                               String end, List<Long> imeis) {
        int startInt, endInt;
        try {
            endInt = Integer.parseInt(end);
            startInt = Integer.parseInt(start);
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("start, end should like mmdd");
        }
        Map<Long, Map<String, Integer>> imeiMap = new HashMap<>();
        try (Table table = connection.getTable(TableName.valueOf("alarm_count"))){
            List<Get> getList = new ArrayList<>();
            for (Long imei : imeis) {
                Get get = new Get(Bytes.toBytes(Long.toString(imei)));
                get.addFamily(Bytes.toBytes("s"));
                getList.add(get);
            }
            Result[] results = table.get(getList);

            for (Result result : results) {
                List<Cell> cells = result.listCells();
                if (cells == null) continue;
                for (Cell cell : cells) {
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String date = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String statusList = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    try {
                        int dateInt = Integer.parseInt(date);
                        if (Utils.dateBetween(startInt, dateInt, endInt)) {
                            long imei = Long.parseLong(row);
                            if (! imeiMap.containsKey(imei)) {
                                imeiMap.put(imei, new HashMap<>());
                            }
                            Map<String, Integer> statusMap = imeiMap.get(imei);
                            String[] statusKV = statusList.split(",");
                            for (String kv : statusKV) {
                                String k = kv.split(":")[0];
                                int v = Integer.parseInt(kv.split(":")[1]);
                                statusMap.put(k, statusMap.getOrDefault(k, 0) + v);
                            }
                        }
                    } catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
        return imeiMap;
    }

    public Map<Long, Map<String, Integer>> getAlarmCountByRead(Connection connection, String start,
                                                                 String end, List<Long> imeis) {
        int startInt, endInt;
        try {
            endInt = Integer.parseInt(end);
            startInt = Integer.parseInt(start);
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("start, end should like mmdd");
        }
        Map<Long, Map<String, Integer>> imeiMap = new HashMap<>();
        try (Table table = connection.getTable(TableName.valueOf("alarm_count"))){
            List<Get> getList = new ArrayList<>();
            for (Long imei : imeis) {
                Get get = new Get(Bytes.toBytes(Long.toString(imei)));
                get.addFamily(Bytes.toBytes("r"));
                getList.add(get);
            }
            Result[] results = table.get(getList);

            for (Result result : results) {
                List<Cell> cells = result.listCells();
                if (cells == null) continue;
                for (Cell cell : cells) {
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String date = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String statusList = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    try {
                        int dateInt = Integer.parseInt(date);
                        if (Utils.dateBetween(startInt, dateInt, endInt)) {
                            long imei = Long.parseLong(row);
                            if (! imeiMap.containsKey(imei)) {
                                imeiMap.put(imei, new HashMap<>());
                            }
                            Map<String, Integer> statusMap = imeiMap.get(imei);
                            String[] statusKV = statusList.split(",");
                            for (String kv : statusKV) {
                                String k = kv.split(":")[0];
                                int v = Integer.parseInt(kv.split(":")[1]);
                                statusMap.put(k, statusMap.getOrDefault(k, 0) + v);
                            }
                        }
                    } catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
        return imeiMap;
    }

    @Override
    public AlarmScanner queryAlarmByUserC(Connection hbase, java.sql.Connection ignite, int userCId, int sortType, QueryFilter filter) {
        HashMap<Integer, List<Long>> map = null;
        // TODO 找到userCID可以访问的所有IMEI，以及他直接相关的C端用户id
        return queryAlarmByImei(hbase, map, sortType, filter);
    }
/*
	@Override
	public Map<String, Integer> groupCountByImeiStatus(java.sql.Connection connection, int parentBId, boolean recursive) {
		Map<String, Integer> map = new HashMap<>();
		ArrayList<Long> imeilist = new ArrayList<>();
		try {
            if (!recursive) {
                String sql = "select imei from device where user_id = " + String.valueOf(parentBId);
                PreparedStatement pstmt =connection.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    imeilist.add(rs.getLong("imei"));
                }
            } else {
                Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance().getChildrenDevicesOfUserB(connection, parentBId);
                for (Integer id : idandimei.keySet()) {
                    List<Long> temp = idandimei.get(id);
                    imeilist.addAll(temp);
                }
            }
            for (int i = 0; i < imeilist.size(); i++) {
                List<IAlarm> ialarmlist;
                Date endtime = new Date();
                ialarmlist = getAlarms(imeilist.get(i), imeilist.get(i), new Date(Settings.BASETIME), endtime);
                String temp = ialarmlist.get(i).getStatus();
                if (map.containsKey(temp))
                    map.replace(temp, map.get(temp), map.get(temp) + 1);
                else
                    map.put(temp, 1);
            }
        }catch (SQLException e){
		    e.printStackTrace();
        }

		return map;
	}
*/
	@Override
	public Map<String, Integer> groupCountByUserIdViewed(java.sql.Connection connection, ArrayList<Integer> parentBIds,
                                                         boolean recursive) {
		Map<String, Integer> map = new HashMap<>();
		try {
            if (!recursive) {
                String sql = "select imei,user_b_id from device where user_b_id in (" + Serialization.listToStr(parentBIds)
                        + ")";
                Map<Long, Integer> imeimap = new HashMap<>();
                PreparedStatement pstmt =connection.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    imeimap.put(rs.getLong("imei"), rs.getInt("user_b_id"));
                }

                for (int parentBid : parentBIds) {
                    int count1 = 0, count2 = 0;
                    for (Long imei : imeimap.keySet()) {
                        if (imeimap.get(imei).equals(parentBid)) {
                            int viewedCount = IgniteSearch.getInstance().getViewedCount(imei);
                            count1 = count1 + viewedCount;
                            count2 = count2 + IgniteSearch.getInstance().getAlarmCount(imei)
                                    - viewedCount;
                        }
                    }
                    String parentBId1, parentBId2 ;
                    parentBId1 = parentBid + "-1";
                    parentBId2 = parentBid + "-0";
                    map.put(parentBId1, count1);
                    map.put(parentBId2, count2);
                }
                return map;
            } else {
                for (int parentid : parentBIds) {
                    int count1 = 0, count2 = 0;
                    Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance().
                            getChildrenDevicesOfUserB(connection, parentid);
                    for (Integer id : idandimei.keySet()) {
                        List<Long> temp = idandimei.get(id);
                        for (Long imei : temp) {
                            int viewedCount = IgniteSearch.getInstance().getViewedCount(imei);
                            count1 = count1 + viewedCount;
                            count2 = count2 + IgniteSearch.getInstance().getAlarmCount(imei)
                                    - viewedCount;
                        }
                    }
                    String parentBId1, parentBId2 ;
                    parentBId1 = Integer.valueOf(parentid).toString() + "-1";
                    parentBId2 = Integer.valueOf(parentid).toString() + "-0";
                    map.put(parentBId1, count1);
                    map.put(parentBId2, count2);
                }
                return map;
            }
        }catch (SQLException e){
		    e.printStackTrace();
        }
		return null;
	}

	@Override
	public Map<Integer, Integer> groupCountByUserId(java.sql.Connection connection, ArrayList<Integer> parentBIds,
                                                    boolean recursive, int topK) {
		Map<Integer, Integer> map = new HashMap<>();
		try {
            if (!recursive) {
                String sql = "select imei,user_b_id from device where user_b_id in (" + Serialization.listToStr(parentBIds)
                        + ")";
                Map<Long, Integer> imeimap = new HashMap<>();
                PreparedStatement pstmt = connection.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    imeimap.put(rs.getLong("imei"), rs.getInt("user_b_id"));
                }

                for (int parentBid: parentBIds) {
                    int count = 0;
                    for (Long imei : imeimap.keySet()) {
                        if (imeimap.get(imei).equals(parentBid)) {
                            count = count + IgniteSearch.getInstance().getAlarmCount(imei);
                        }
                    }
                    map.put(parentBid, count);
                }
                return map;
            } else {
                for (int parentid : parentBIds) {
                    int count = 0;
                    Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance()
                            .getChildrenDevicesOfUserB(connection, parentid);
                    for (Integer id : idandimei.keySet()) {
                        List<Long> temp = idandimei.get(id);
                        for (Long imei : temp) {
                            count = count + IgniteSearch.getInstance().getAlarmCount(imei);
                        }
                    }
                    map.put(parentid, count);
                }
                return map;
            }
        }catch (SQLException e){
		    e.printStackTrace();
        }
		return null;
	}
}
