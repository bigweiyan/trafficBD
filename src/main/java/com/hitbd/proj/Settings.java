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
    public static String TABLES[] = {"alarm_0730", "alarm_0803", "alarm_0807",
        "alarm_0811", "alarm_0815", "alarm_0819", "alarm_0823", "alarm_0827",
        "alarm_0831"};
    public static long START_TIME = 1532880000000L; // 2018-07-30 00:00:00
    public static long END_TIME = 1535990399000L; // 2018-09-03 23:59:59
    public static int MAX_DEVICES_PER_QUERY = 100;
    public static Configuration HBASE_CONFIG = null;
    public static int MAX_THREAD = 5;
    public static int MAX_CACHE_ALARM = 100;
}
