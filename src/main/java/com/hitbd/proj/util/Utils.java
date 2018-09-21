package com.hitbd.proj.util;

import com.hitbd.proj.Settings;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

    public static int getRelativeDate(Date date) {
        long milliSecond = date.getTime() - Settings.BASETIME;
        int dayPass = (int) (milliSecond / (1000L * 3600* 24));
        return dayPass;
    }

    public static long getBasedTime(String tableName){
        if (!tableName.startsWith("alarm_")) throw new IllegalArgumentException("argument should like \"alarm_MMdd\"");
        int month = Integer.valueOf(tableName.substring(6,8)) - 1;
        int day = Integer.valueOf(tableName.substring(8));
        Calendar calendar = Calendar.getInstance();
        int year = month <= calendar.get(Calendar.MONTH) ? calendar.get(Calendar.YEAR) : calendar.get(Calendar.YEAR) - 1;
        calendar.set(year, month, day, 0,0,0);
        return calendar.getTime().getTime();
    }

    public static ArrayList<String> getUseTable(Date queryStart, Date queryEnd) {
        if (queryEnd != null && queryStart != null && queryEnd.before(queryStart)) return new ArrayList<>();
        Date tableStart = new Date(Settings.START_TIME);
        Date tableEnd = new Date(Settings.END_TIME);
        int startPos = 0, endPos = Settings.TABLES.length;
        if (queryStart != null && tableStart.before(queryStart)) {
            String queryStartTable = Utils.getTableName(queryStart);
            for (String tableName: Settings.TABLES) {
                if (!queryStartTable.equals(tableName)) {
                    startPos ++;
                }else {
                    break;
                }
            }
        }
        if (queryEnd != null && tableEnd.after(queryEnd)) {
            String queryEndTable = Utils.getTableName(queryEnd);
            for (int i = Settings.TABLES.length - 1; i >= 0; i--) {
                if (!queryEndTable.equals(Settings.TABLES[i])) {
                    endPos --;
                }else {
                    break;
                }
            }
        }
        ArrayList<String> result = new ArrayList<>();
        for (; startPos < endPos; startPos++) {
            result.add(Settings.TABLES[startPos]);
        }
        return result;
    }
}
