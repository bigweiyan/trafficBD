package com.hitbd.proj.util;

import com.hitbd.proj.Settings;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;

public class Utils {
    /**
     * A5.3
     * 获取Hbase的Alarm表名
     * @param date 该Alarm的创建时间
     * @return 对应的表名
     */
    public static String getTableName(Date date) {
        long milliSecond = date.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date result = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMdd");
        return "alarm_" + simpleDateFormat.format(result);
    }

    public static String getRelativeSecond(Date date) {
        long milliSecond = date.getTime() - Settings.BASETIME;
        Date nearestPivot = new Date(Settings.BASETIME + milliSecond / (1000L * 3600 * 24 * 4) * (1000L * 3600 * 24 * 4));
        long relativeSec = (date.getTime() - nearestPivot.getTime()) / 1000;

        Formatter formatter = new Formatter();
        return formatter.format("%05x", relativeSec).toString();
    }
}
