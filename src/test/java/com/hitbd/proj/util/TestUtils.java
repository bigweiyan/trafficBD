package com.hitbd.proj.util;

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
}
