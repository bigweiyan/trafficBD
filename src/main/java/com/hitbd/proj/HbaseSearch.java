package com.hitbd.proj;

import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.logic.Query;
import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HbaseSearch implements IHbaseSearch {

    private static Connection connection;
    private static HbaseSearch search;

    private HbaseSearch(){};
    public static HbaseSearch getInstance() {
        if (search == null) search = new HbaseSearch();
        return search;
    }

    @Override
    public boolean connect() {
        if (connection == null || connection.isClosed()) {
            try {
                if (Settings.HBASE_CONFIG == null)
                    Settings.HBASE_CONFIG = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean connect(Configuration config) {
        if (connection == null || connection.isClosed()){
            try {
                Settings.HBASE_CONFIG = config;
                connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
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

    public ResultScanner scanTable(String tableName,String start,String end) {
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

    public void addToList(ResultScanner results, Date startTime, Date endTime, List<IAlarm> ret,long basicTime) {
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

    @Override
    public void insertAlarm(List<IAlarm> alarms) throws TimeException, ForeignKeyException {
        for(IAlarm alarm:alarms) {
            //异常抛出
            String tableName = alarm.getTableName();

            StringBuilder sb = new StringBuilder();
            String imeistr = String.valueOf(alarm.getImei());
            for (int j = 0; j < 17 - imeistr.length(); j++) {
                sb.append(0);
            }
            sb.append(imeistr).append(Utils.getRelativeSecond(alarm.getCreateTime())).append((IgniteSearch.getInstance().getAlarmCount(alarm.getImei())+1)%10);
            String rowkey = sb.toString();

            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowkey));
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
    public void setPushTime(List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException {
        for(Pair<String,String> rowKey:rowKeys) {
            //异常抛出
            String tableName = rowKey.getKey();
            String rowkey = rowKey.getValue();

            Table table;
            try {
                table = connection.getTable(TableName.valueOf(tableName));

                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                byte[] value = result.getValue("r".getBytes(), "record".getBytes());
                String record = Bytes.toString(value);
                CSVParser csvparser = CSVParser.parse(record, CSVFormat.DEFAULT);
                List<CSVRecord> csvrecord = csvparser.getRecords();
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                StringBuilder sb = new StringBuilder();
                sb.append('\"').append(csvrecord.get(0).get(0)).append("\",").append(csvrecord.get(0).get(1)).append(',').append(csvrecord.get(0).get(2)).append(',');
                sb.append(csvrecord.get(0).get(3)).append(',').append(csvrecord.get(0).get(4)).append(',').append(dateformatter.format(pushTime)).append(',').append(csvrecord.get(0).get(6));

                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn("r".getBytes(), "record".getBytes(), sb.toString().getBytes());
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
    public AlarmScanner queryAlarmByUser(int queryUser, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        // 存放用户及其对应设备
        Map<Integer, List<Long>> userAndDevice;
        // 读取用户及其对应设备imei,这些设备将被过期时间进行过滤
        if (recursive) {
            userAndDevice = IgniteSearch.getInstance().getChildrenDevicesOfUserB(queryUser, false);
        } else {
            userAndDevice = new HashMap<>();
            for (int user : userBIds) userAndDevice.put(user, IgniteSearch.getInstance().getDirectDevices(user, queryUser, false));
        }

        // 计算需要在哪些表中进行查询
        List<String> usedTable;
        if (filter.getAllowTimeRange() == null) {
            usedTable = Arrays.asList(Settings.TABLES);
        }else{
            usedTable = Utils.getUseTable(filter.getAllowTimeRange().getKey(), filter.getAllowTimeRange().getValue());
        }

        AlarmScanner result = new AlarmScanner();
        // 划分查询，每个查询按时间进行排列
        LinkedList<Query> queries = new LinkedList<>();
        if (sortType == HbaseSearch.SORT_BY_CREATE_TIME || sortType == HbaseSearch.NO_SORT) {
            for (int i = 0; i < usedTable.size(); i++){
                // 确定这个查询所对应的起止时间
                String startRelativeSecond;
                String endRelativeSecond;
                if (i == 0) {
                    startRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getKey());
                }else {
                    startRelativeSecond = "00000";
                }
                if (i == usedTable.size() - 1) {
                    endRelativeSecond = Utils.getRelativeSecond(filter.getAllowTimeRange().getValue());
                }else {
                    endRelativeSecond = "fffff";
                }
                // 新增查询
                Query query = new Query();
                query.order = SORT_BY_CREATE_TIME;
                query.tableName = usedTable.get(i);
                query.startRelativeSecond = startRelativeSecond;
                query.endRelativeSecond = endRelativeSecond;
                List<Pair<Integer, Long>> imeis = new ArrayList<>();
                for (Map.Entry<Integer, List<Long>> user : userAndDevice.entrySet()) {
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
                            if (lastPostion % Settings.MAX_DEVICES_PER_QUERY == 0) {
                                doneUser = false;
                                break;
                            }
                        }
                        // 如果一个表中查询的设备数大于100， 则构建一个新查询
                        if (imeis.size() > Settings.MAX_DEVICES_PER_QUERY) {
                            query.imeis = imeis;
                            queries.add(query);
                            query = new Query();
                            query.order = SORT_BY_CREATE_TIME;
                            query.tableName = usedTable.get(i);
                            query.startRelativeSecond = startRelativeSecond;
                            query.endRelativeSecond = endRelativeSecond;
                            imeis = new ArrayList<>();
                        }
                    }
                }
                query.imeis = imeis;
                queries.add(query);
                result.setQueries(queries);
            }
        }else if (sortType == HbaseSearch.SORT_BY_IMEI) {
            // TODO solve imei sort
            return null;
        }else if (sortType == HbaseSearch.SORT_BY_USER_ID) {
            // TODO solve user_id sort
            return null;
        }else {
            throw new IllegalArgumentException("sort type should be defined in IHbaseSearch");
        }
        result.setQueries(queries);
        return result;
    }

    @Override
    public AlarmScanner queryAlarmByImei(HashMap<Integer, List<Long>> userAndDevices, int sortType, QueryFilter filter) {
        AlarmScanner result = new AlarmScanner();
        // TODO
        return result;
    }

    @Override
    public void asyncQueryAlarmByUser(int qid, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        // TODO
    }

    @Override
    public void asyncQueryAlarmByImei(int qid, List<Long> imeis, int sortType, QueryFilter filter) {
        // TODO
    }

    @Override
    public List<IAlarm> queryAlarmByUserC(int userCId, int sortType) {
        return null;
    }

	@Override
	public Map<String, Integer> groupCountByImeiStatus(int parentBId, boolean recursive) {
		Map<String, Integer> map = new HashMap<String, Integer>();
//		ArrayList<Long> imeilist = new ArrayList<Long>();
//		if (recursive == false) {
//			String sql = "select imei from device where user_id = " + String.valueOf(parentBId);
//			PreparedStatement pstmt = connection.prepareStatement(sql);
//			ResultSet rs = pstmt.executeQuery();
//			while (rs.next()) {
//				imeilist.add(rs.getLong("imei"));
//			}
//		} else {
//			Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance().getChildrenDevicesOfUserB(parentBId);
//			for (Integer id : idandimei.keySet()) {
//				List<Long> temp = idandimei.get(id);
//				for (Long i : temp) {
//					imeilist.add(i);
//				}
//			}
//		}
//		for (int i = 0; i < imeilist.size(); i++) {
//			List<IAlarm> ialarmlist = new ArrayList<IAlarm>();
//			Date endtime = new Date();
//			ialarmlist = getAlarms(imeilist.get(i), imeilist.get(i), new Date(Settings.BASETIME), endtime);
//			String temp = ialarmlist.get(i).getStatus();
//			if (map.containsKey(temp) == true)
//				map.replace(temp, map.get(temp), map.get(temp) + 1);
//			else
//				map.put(temp, 1);
//		}
		return map;
	}

	@Override
	public Map<String, Integer> groupCountByUserIdViewed(ArrayList<Integer> parentBIds, boolean recursive) {
//		Map<String, Integer> map = new HashMap<String, Integer>();
//		if (recursive == false) {
//			String sql = "select imei,user_b_id from device where user_b_id in (" + Serialization.listToStr(parentBIds)
//					+ ")";
//			Map<Long, Integer> imeimap = new HashMap<Long, Integer>();
//			PreparedStatement pstmt = connection.prepareStatement(sql);
//			ResultSet rs = pstmt.executeQuery();
//			while (rs.next()) {
//				imeimap.put(rs.getLong("imei"), rs.getInt("user_b_id"));
//			}
//
//			for (int i = 0; i < parentBIds.size(); i++) {
//				int count1 = 0, count2 = 0;
//				for (Long imei : imeimap.keySet()) {
//					if (imeimap.get(imei).equals(parentBIds.get(i))) {
//						count1 = count1 + IgniteSearch.getInstance().getViewedCount(imei);
//						count2 = count2 + IgniteSearch.getInstance().getAlarmCount(imei)
//								- IgniteSearch.getInstance().getViewedCount(imei);
//					}
//				}
//				String parentBId1 = new String(), parentBId2 = new String();
//				parentBId1 = parentBIds.get(i).toString() + "1";
//				parentBId2 = parentBIds.get(i).toString() + "0";
//				map.put(parentBId1, count1);
//				map.put(parentBId2, count2);
//			}
//			return map;
//		} else {
//			for (int parentid : parentBIds) {
//				int count1 = 0, count2 = 0;
//				Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance().getChildrenDevicesOfUserB(parentid);
//				for (Integer id : idandimei.keySet()) {
//					List<Long> temp = idandimei.get(id);
//					for (Long imei : temp) {
//						count1 = count1 + IgniteSearch.getInstance().getViewedCount(imei);
//						count2 = count2 + IgniteSearch.getInstance().getAlarmCount(imei)
//								- IgniteSearch.getInstance().getViewedCount(imei);
//					}
//				}
//				String parentBId1 = new String(), parentBId2 = new String();
//				parentBId1 = Integer.valueOf(parentid).toString() + "1";
//				parentBId2 = Integer.valueOf(parentid).toString() + "0";
//				map.put(parentBId1, count1);
//				map.put(parentBId2, count2);
//			}
//			return map;
//		}
		return null;
	}

	@Override
	public Map<Integer, Integer> groupCountByUserId(ArrayList<Integer> parentBIds, boolean recursive, int topK) {
//		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
//		if (recursive == false) {
//			String sql = "select imei,user_b_id from device where user_b_id in (" + Serialization.listToStr(parentBIds)
//					+ ")";
//			Map<Long, Integer> imeimap = new HashMap<Long, Integer>();
//			PreparedStatement pstmt = connection.prepareStatement(sql);
//			ResultSet rs = pstmt.executeQuery();
//			while (rs.next()) {
//				imeimap.put(rs.getLong("imei"), rs.getInt("user_b_id"));
//			}
//
//			for (int i = 0; i < parentBIds.size(); i++) {
//				int count = 0;
//				for (Long imei : imeimap.keySet()) {
//					if (imeimap.get(imei).equals(parentBIds.get(i))) {
//						count = count + IgniteSearch.getInstance().getAlarmCount(imei);
//					}
//				}
//				map.put(parentBIds.get(i), count);
//			}
//			return map;
//		} else {
//			for (int parentid : parentBIds) {
//				int count = 0;
//				Map<Integer, List<Long>> idandimei = IgniteSearch.getInstance().getChildrenDevicesOfUserB(parentid);
//				for (Integer id : idandimei.keySet()) {
//					List<Long> temp = idandimei.get(id);
//					for (Long imei : temp) {
//						count = count + IgniteSearch.getInstance().getAlarmCount(imei);
//					}
//				}
//				map.put(parentid, count);
//			}
//			return map;
//		}
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
