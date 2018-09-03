package com.hitbd.proj.util;

import com.hitbd.proj.Settings;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TestUtils {
    @Test
    public void testAlarmName(){
        Date date = new Date(1262707140000L); //2010-01-05 23:59:00
        Assert.assertEquals("alarm_0105", Utils.getTableName(date));
        date = new Date(1262620800000L); //2010-01-05 00:00:00
        Assert.assertEquals("alarm_0105", Utils.getTableName(date));
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
}
