package com.hitbd.proj;

import org.apache.hadoop.conf.Configuration;

import java.util.Date;

public class Settings {
    public static Configuration HBASE_CONFIG = null;
    public static String LOG_DIR = "./";

    public static int ROW_KEY_E_FACTOR = 9;

    public static long BASETIME = 1262275200000L; //2010年1月1日 00:00:00
    public static Date MAX_TIME = new Date(4102415999000L); // 2099-12-31 23:59:59
    public static String TABLES[] = {"alarm_0628", "alarm_0702", "alarm_0706",
            "alarm_0710", "alarm_0714", "alarm_0718", "alarm_0722", "alarm_0726",
            "alarm_0730", "alarm_0803", "alarm_0807", "alarm_0811", "alarm_0815",
            "alarm_0819", "alarm_0823", "alarm_0827", "alarm_0831"};
    public static long START_TIME = 1530115200000L; // 2018-06-28 00:00:00
    public static long END_TIME = 1535990399000L; // 2018-09-03 23:59:59

    public static int MAX_DEVICES_PER_WORKER = 100;
    public static int MAX_WORKER_THREAD = 3;
    public static int MAX_CACHE_ALARM = 1000;
    public static long IMPORT_TIME_SHIFT = 0L;

    public static class Test{
        public static int IMEI_PER_QUERY = 100;
        public static int USER_PER_QUERY = 10;
        public static int QUERY_THREAD_PER_TEST = 3;
        public static boolean WAIT_UNTIL_FINISH = false;
        public static long START_TIME = 1532880000000L; // 2018-07-30 00:00:00
        public static long END_TIME = 1534089600000L; // 2018-08-13 00:00:00
        public static boolean SHOW_TOP_RESULT = false;
        public static boolean SHOW_ALL_RESULT = false;
        public static int RESULT_SIZE = 100;
    }
}
