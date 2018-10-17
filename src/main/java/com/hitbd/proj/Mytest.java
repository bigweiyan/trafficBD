package com.hitbd.proj;

import com.hitbd.proj.util.Serialization;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class Mytest {
    private static java.sql.Connection ignite;
    private static Connection connection;

    public static void main(String[] args) {
        Main.loadSettings();
        System.out.println("初始化Ignite");
        IgniteSearch.getInstance();
        System.out.println("初始化完成");
        try {
            ignite = DriverManager.getConnection("jdbc:ignite:thin://localhost");
            connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            return;
        }
        switch (args[0]) {
            case "getdata":
                getAlarmCountByStatus();
                System.out.println("yes");
                break;
            case "testcountmonth":
                testCountMonth();
                System.out.println("yes");
                break;
            /*case "testover":
                testCountOverSpeed();
                break;*/
            case "testcountstatus":
                testCountImei();
                break;
            case "gettopten":
                testGetTop();
                System.out.println("yes");
                break;
            /*case "getnotviewdalarm":
                testNotViewd();
                System.out.println("yes");
                break;
            case "iscontain":
                testIsContains();
                System.out.println("yes");
                break;*/
            case "countday":
                testCountDay();
                System.out.println("yes");
                break;
            default:
                System.out.println("shell");
        }
    }
    public static void getAlarmCountByStatus(){
        String temp = "/data1/yy/all";
        String treefile = "/data1/yy/alarmall";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(treefile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(temp))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                String[] items = linetxt.split(",");
                Long imei = Long.valueOf(items[2]);
                ArrayList<Long> imeilist = new ArrayList<>();
                imeilist.add(imei);
                Map<Long, Map<String, Integer>> map = HbaseSearch.getInstance().getAlarmCountByStatus(connection, "0801", "0831", imeilist);
                int i = 0;
                Map<String,Integer> status = map.get(imei);
                String all=items[0]+","+items[1]+","+items[2];
                if(status!=null) {
                    for (String id : status.keySet()) {
                        all = all + "," + id + "," + status.get(id);
                    }
                    bw.write(all);
                    bw.newLine();
                    bw.flush();
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("成功计算得到所有告警的摘要");
    }
    /**
     * test1、测试某段时间内的超速告警次数
     */
    public static void testCountOverSpeed(){
        String inputfile = "/data1/yy/test/1.csv";
        String outputfile = "/data1/yy/result/test1.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                String imei = linetxt;
                if((linetxt = buffer.readLine())!=null) {
                    String userid = linetxt;
                    Long start = new Date().getTime();
                    int total = countOverSpeedTimes(imei,userid);
                    long time = new Date().getTime() - start;
                    bw.write(total+","+time);
                    bw.newLine();
                    bw.flush();
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 1、统计某段时间内的超速告警次数
     */
    public static int countOverSpeedTimes(String imei, String userid) {
        //先找到满足所有条件的imei号
        ArrayList<Long> tempimeilist = Serialization.longToList(imei);
        ArrayList<Long> imeilist = new ArrayList<Long>();
        ArrayList<Integer> userlist = Serialization.strToList(userid);
        for (int i = 0; i < userlist.size(); i++) {
            List<Long> userimeilist;
            userimeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userlist.get(i), 0, false);
            for (Long id : tempimeilist) {
                for (Long ui : userimeilist) {
                    if (ui.equals(id))
                        imeilist.add(ui);
                }
            }
        }
        //根据imei和过滤条件进行查询
        String starttime = "0801";
        String endtime = "0831";
        Map<Long, Map<String, Integer>> map = HbaseSearch.getInstance().getAlarmCountByStatus(connection, starttime, endtime, imeilist);
        int total = 0;
        for (Long id : map.keySet()) {
            Map<String, Integer> temp = map.get(id);
            for(String tp:temp.keySet()){
                if(tp.equals("overSpeed"))
                    total = total +  temp.get(tp);
                if(tp.equals("6"))
                    total = total + temp.get(tp);
            }
        }
        return total;
    }
    /**
     * 测试统计数据
     */
    public static void testCountImei(){
        String inputfile = "/data1/yy/test/2.csv";
        String outputfile = "/data1/yy/result/test2.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                if((linetxt = buffer.readLine())!=null) {
                    String imei = linetxt;
                    if((linetxt = buffer.readLine())!=null) {
                        String userid = linetxt;
                        if((linetxt = buffer.readLine())!=null) {
                            String  parent_id = linetxt;
                            Long start = new Date().getTime();
                            int size = countImeiStatus(imei,userid,parent_id,"");
                            Long time = new Date().getTime() - start;
                            bw.write(size+","+time.toString());
                            bw.newLine();
                            bw.flush();
                        }
                    }
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 3、按照imei和告警状态统计
     */
    public static Integer countImeiStatus(String imei, String userid, String parentid, String status) {
        //首先得到要查询的imei号
        ArrayList<Integer> tempuserlist = Serialization.strToList(userid);
        ArrayList<Integer> userlist = new ArrayList<>();
        ArrayList<Long> tempimeilist = Serialization.longToList(imei);
        ArrayList<Long> imeilist = new ArrayList<>();
        String sql = "select user_id from user_b where parent_id like '" + parentid+"'";
        try {
            PreparedStatement pstmt = ignite.prepareStatement(sql);
            ResultSet userrs = pstmt.executeQuery();
            while (userrs.next()) {
                Integer temp = userrs.getInt("user_id");
                if (tempuserlist.contains(temp))
                    userlist.add(temp);
            }
            pstmt.close();
            userrs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < userlist.size(); i++) {
            List<Long> userimeilist;
            userimeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userlist.get(i), 0, false);
            for (Long id : tempimeilist) {
                for (Long ui : userimeilist) {
                    if (ui.equals(id))
                        imeilist.add(ui);
                }
            }
        }
        //根据imei和状态进行过滤
        String starttime = "0801";
        String endtime = "0802";
        Map<Long, Map<String, Integer>> map = HbaseSearch.getInstance().getAlarmCountByStatus(connection, starttime, endtime, imeilist);
        return map.size();
    }

    /**
     * 测试求解前十
     */
    public static void testGetTop(){
        String inputfile = "/data1/yy/test/2.csv";
        String outputfile = "/data1/yy/result/test3.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                if((linetxt = buffer.readLine())!=null) {
                    String imei = linetxt;
                    if((linetxt = buffer.readLine())!=null) {
                        String userid = linetxt;
                        if((linetxt = buffer.readLine())!=null) {
                            String  parent_id = linetxt;
                            Long start = new Date().getTime();
                            Integer total = getTopTen(userid,parent_id);
                            Long time = new Date().getTime() - start;
                            bw.write(total +","+time);
                            bw.newLine();
                            bw.flush();
                        }
                    }
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 4、获取用户告警记录排行前十的设备
     */
    public static Integer getTopTen(String userid, String parentid) {
        //首先得到要查询的imei号
        ArrayList<Integer> tempuserlist = Serialization.strToList(userid);
        ArrayList<Integer> userlist = new ArrayList<>();
        ArrayList<Long> imeilist = new ArrayList<>();
        String sql = "select user_id from user_b where parent_id like '" + parentid+"'";
        try {
            PreparedStatement pstmt = ignite.prepareStatement(sql);
            ResultSet userrs = pstmt.executeQuery();
            while (userrs.next()) {
                Integer temp = userrs.getInt("user_id");
                if (tempuserlist.contains(temp))
                    userlist.add(temp);
            }
            pstmt.close();
            userrs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < userlist.size(); i++) {
            List<Long> userimeilist;
            userimeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userlist.get(i), 0, false);
            for (Long id : userimeilist) {
                imeilist.add(id);
            }
        }
        //根据imei查询告警
        String starttime = "0805";
        String endtime = "0806";
        Map<Long, Integer> map = HbaseSearch.getInstance().getAlarmCount(connection, starttime, endtime, imeilist);
        Map<Long, Integer> sortedmap = map;//sortMapByValue(map);
        int count = 0;
        int total = 0;
        for (Long id : sortedmap.keySet()) {
            count++;
            total = total + sortedmap.get(id);
            if (count == 10)
                break;
        }
        return map.size();
    }


    public static Map<Long, Integer> sortMapByValue(Map<Long, Integer> oriMap) {
        Map<Long, Integer> sortedMap = new LinkedHashMap<Long, Integer>();
        if (oriMap != null && !oriMap.isEmpty()) {
            List<Map.Entry<Long, Integer>> entryList = new ArrayList<>(oriMap.entrySet());
            Collections.sort(entryList,
                    new Comparator<Map.Entry<Long, Integer>>() {
                        public int compare(Map.Entry<Long, Integer> entry1,
                                           Map.Entry<Long, Integer> entry2) {
                            int value1 = 0, value2 = 0;
                            value1 = entry1.getValue();
                            value2 = entry2.getValue();
                            return value2 - value1;
                        }
                    });
            Iterator<Map.Entry<Long, Integer>> iter = entryList.iterator();
            Map.Entry<Long, Integer> tmpEntry = null;
            while (iter.hasNext()) {
                tmpEntry = iter.next();
                sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
            }
        }
        return sortedMap;
    }

    /**
     * 测试聚集未读告警
     */
    public static void testNotViewd(){
        String inputfile = "/data1/yy/test/2.csv";
        String outputfile = "/data1/yy/result/test4.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                if((linetxt = buffer.readLine())!=null) {
                    String imei = linetxt;
                    if((linetxt = buffer.readLine())!=null) {
                        String userid = linetxt;
                        if((linetxt = buffer.readLine())!=null) {
                            String  parent_id = linetxt;
                            Long start = new Date().getTime();
                            Integer total = getNotViewdCount(imei,userid,parent_id);
                            Long time = new Date().getTime() - start;
                            bw.write(total +","+time);
                            bw.newLine();
                            bw.flush();
                        }
                    }
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 6、获取未读告警记录数
     */
    public static Integer getNotViewdCount(String imei, String userid, String parentid) {
        //首先得到要查询的imei号
        ArrayList<Integer> tempuserlist = Serialization.strToList(userid);
        ArrayList<Integer> userlist = new ArrayList<>();
        ArrayList<Long> tempimeilist = Serialization.longToList(imei);
        ArrayList<Long> imeilist = new ArrayList<>();
        String sql = "select user_id from user_b where parent_id like '" + parentid+"'";
        try {
            PreparedStatement pstmt = ignite.prepareStatement(sql);
            ResultSet userrs = pstmt.executeQuery();
            while (userrs.next()) {
                Integer temp = userrs.getInt("user_id");
                if (tempuserlist.contains(temp))
                    userlist.add(temp);
            }
            pstmt.close();
            userrs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < userlist.size(); i++) {
            List<Long> userimeilist;
            userimeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userlist.get(i), 0, false);
            for (Long id : tempimeilist) {
                for (Long ui : userimeilist) {
                    if (ui.equals(id))
                        imeilist.add(ui);
                }
            }
        }
        //根据imei和状态进行过滤
        String starttime = "0801";
        String endtime = "0831";
        Map<Long, Map<String, Integer>> map = HbaseSearch.getInstance().getAlarmCountByRead(connection, starttime, endtime, imeilist);
        int total = 0;
        for (Long id : map.keySet()) {
            Map<String, Integer> temp = map.get(id);
            if(temp.containsKey("0"))
                total = total + temp.get("0");
        }
        return total;
    }

    /**
     * 测试7
     */
    public static void testCountDay(){
        String inputfile = "/data1/yy/test/3.csv";
        String outputfile = "/data1/yy/result/test5.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                Integer userid = Integer.valueOf(linetxt);
                Long start = new Date().getTime();
                Long[] result = countDayImeiCount(userid);
                Long time = new Date().getTime() - start;
                bw.write(result[2]+","+result[0]+","+result[1] +","+time);
                bw.newLine();
                bw.flush();
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * 7、按用户统计最近一天告警设备数
     */
    public static Long[] countDayImeiCount(int userid) {
        String starttime = "0810";
        String endtime = "0811";
        Long time1 = new Date().getTime();
        List<Long> imeilist;
        imeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userid, 0, false);
        Long time2 = new Date().getTime()-time1;
        Long time3 = new Date().getTime();
        Map<Long, Integer> map = HbaseSearch.getInstance().getAlarmCount(connection, starttime, endtime, imeilist);
        Long time4 = new Date().getTime()-time3;
        int countimei = 0, countalarm = 0;
        countimei = imeilist.size();
        for (Long id : map.keySet()) {
            countalarm = countalarm + map.get(id);
        }
        Long size = Long.valueOf(map.size());
        Long[] result = {time2,time4,size};
        return result;
    }

    /**
     * 测试求解前十
     */
    public static void testCountMonth(){
        String inputfile = "/data1/yy/test/2.csv";
        String outputfile = "/data1/yy/result/test6.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                if((linetxt = buffer.readLine())!=null) {
                    String imei = linetxt;
                    if((linetxt = buffer.readLine())!=null) {
                        String userid = linetxt;
                        if((linetxt = buffer.readLine())!=null) {
                            String  parent_id = linetxt;
                            Long start = new Date().getTime();
                            Long[] result = countMonthImeiNum(userid,parent_id);
                            Long time = new Date().getTime() - start;
                            bw.write(result[2]+","+result[0]+","+result[1]+","+time.toString());
                            bw.newLine();
                            bw.flush();
                        }
                    }
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 10、查询一个月内产生告警数据最多的10个imei
     */
    public static  Long[] countMonthImeiNum(String userid, String parentid) {

        //计算得到imei号
        Long time1=new Date().getTime();
        ArrayList<Integer> tempuserlist = Serialization.strToList(userid);
        ArrayList<Integer> userlist = new ArrayList<>();
        ArrayList<Long> imeilist = new ArrayList<>();
        String sql = "select user_id from user_b where parent_id like '" + parentid+"'";
        try {
            PreparedStatement pstmt = ignite.prepareStatement(sql);
            ResultSet userrs = pstmt.executeQuery();
            while (userrs.next()) {
                Integer temp = userrs.getInt("user_id");
                if (tempuserlist.contains(temp))
                    userlist.add(temp);
            }
            pstmt.close();
            userrs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < userlist.size(); i++) {
            List<Long> userimeilist;
            userimeilist = IgniteSearch.getInstance().getDirectDevices(ignite, userlist.get(i), 0, false);
            for (Long ui : userimeilist) {
                imeilist.add(ui);
            }
        }
        Long time2 = new Date().getTime()-time1;
        Long time3 = new Date().getTime();
        //得到一个月以内告警数目最多的10个imei
        String starttime = "0801";
        String endtime = "0831";
        ArrayList<Long> list = new ArrayList<>();
        Map<Long, Integer> map = HbaseSearch.getInstance().getAlarmCount(connection, starttime, endtime, imeilist);
        Map<Long, Integer> sortedmap = sortMapByValue(map);
        Long time4 = new Date().getTime()-time3;
        Long size = Long.valueOf(map.size());
        Long[] result={time2,time4,size};
        return result;
    }

    /**
     * 测试11
     */
    public static void testIsContains(){
        String inputfile = "/data1/yy/test/4.csv";
        String outputfile = "/data1/yy/result/test7.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                Long imei = Long.valueOf(linetxt);
                Long start = new Date().getTime();
                Integer total = isContains(imei);
                Long time = new Date().getTime() - start;
                bw.write(total +","+time);
                bw.newLine();
                bw.flush();
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 11、获取某月中imei是否有告警记录
     */
    public static Integer isContains(Long imei) {
        String starttime = "0801";
        String endtime = "0831";
        ArrayList<Long> list = new ArrayList<>();
        list.add(imei);
        Map<Long, Integer> map = HbaseSearch.getInstance().getAlarmCount(connection, starttime, endtime, list);
        return map.get(imei);
    }
}
