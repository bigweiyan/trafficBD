package com.hitbd.proj;

import org.apache.hadoop.conf.Configuration;

import java.util.Date;

public class Settings {
    public static long BASETIME = 1262275200000L; //2010年1月1日 00:00:00
    public static String QUERY_LOG_FILENAME = "Query%d.txt"; //查询结果文件名
    public static String igniteHostAddress = "127.0.0.1";
    public static String logDir = "./";
    public static Date MAXTIME = new Date(199,1,1);
    public static int ROW_KEY_E_FACTOR = 9;
    public static String TABLES[] = {"alarm_0531", "alarm_0604", "alarm_0608",
        "alarm_0612", "alarm_0616", "alarm_0620", "alarm_0624", "alarm_0628",
        "alarm_0702", "alarm_0706", "alarm_0710", "alarm_0714", "alarm_0718",
        "alarm_0722", "alarm_0726", "alarm_0730", "alarm_0803", "alarm_0807",
        "alarm_0811"};
    public static long START_TIME = 1527696000000L; // 2018-05-31 00:00:00
    public static long END_TIME = 1534262399000L; // 2018-08-14 23:59:59
    public static int MAX_DEVICES_PER_QUERY = 100;
    public static int MAX_ALARMS_PER_QUERY = 10000;
    public static Configuration HBASE_CONFIG = null;
    public static int MAX_THREAD = 5;
}
