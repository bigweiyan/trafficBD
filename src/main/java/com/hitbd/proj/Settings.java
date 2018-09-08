package com.hitbd.proj;

import java.util.Date;

public class Settings {
    public static long BASETIME = 1262275200000L; //2010年1月1日 00:00:00
    public static String QUERY_LOG_FILENAME = "Query%d.txt"; //查询结果文件名
    public static String igniteHostAddress = "127.0.0.1";
    public static String logDir = "./";
    public static Date MAXTIME = new Date(199,1,1);
    public static int ROW_KEY_E_FACTOR = 9;
    public static String TABLES[] = {"0531", "0604", "0608",
        "0612", "0616", "0620", "0624", "0628",
        "0702", "0706", "0710", "0714", "0718",
        "0722", "0726", "0730", "0803", "0807",
        "0811"};
}
