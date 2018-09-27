package com.hitbd.proj.util;

import com.hitbd.proj.Settings;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class UtilsTest {
    @Test
    public void testAlarmName(){
        Date date = new Date(1262707140000L); //2010-01-05 23:59:00
        Assert.assertEquals("alarm_0105", Utils.getTableName(date));
        date = new Date(1533298236000L); //2018-08-03 20:10:36
        Assert.assertEquals("alarm_0803", Utils.getTableName(date));
        date = new Date(1262620799000L); //2010-01-04 23:59:59
        Assert.assertEquals("alarm_0101", Utils.getTableName(date));
        date = new Date(1262966401000L); //2010-01-09 00:00:01
        Assert.assertEquals("alarm_0109", Utils.getTableName(date));
    }

    @Test
    public void testRelativeSecond() {
        Date date = new Date(1262966401000L);
        Assert.assertEquals("00001", Utils.getRelativeSecond(date));
        date = new Date(1262966417000L);
        Assert.assertEquals("00011", Utils.getRelativeSecond(date));
    }

    @Test
    public void testRelativeDate() {
        Date date = new Date(Settings.BASETIME + 1000 * 3600 * 24);
        Assert.assertEquals(1, Utils.getRelativeDate(date));
        date = new Date(Settings.BASETIME + 1000 * 3600 * 24 + 1);
        Assert.assertEquals(1, Utils.getRelativeDate(date));
    }

    @Test
    public void testGetUsedTable() {
        // TODO testGetUsedTable
//        Date start = new Date(Settings.START_TIME);
//        Date end = new Date(Settings.END_TIME + 1000);
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 19);
//        Assert.assertEquals(Utils.getUseTable(start, null).size(), 19);
//        Assert.assertEquals(Utils.getUseTable(null, end).size(), 19);
//        end = new Date(Settings.START_TIME + 1000);
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 1);
//        end = new Date(Settings.START_TIME - 1000);
//        start = new Date(Settings.START_TIME - 2000);
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 0);
//        start = new Date(Settings.END_TIME + 1000);
//        end = new Date(Settings.END_TIME + 2000);
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 0);
//
//        start = new Date(1528128000000L); // 2018-06-05 00:00:00
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 18);
//        end = new Date(1533312000000L); // 2018-08-04 00:00:00
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 16);
//        Assert.assertEquals(Utils.getUseTable(start, start).size(), 1);
//        end = new Date(1528127999000L); // 2018-06-04 23:59:59
//        Assert.assertEquals(Utils.getUseTable(start, end).size(), 0);
    }
}
