package com.hitbd.proj.model;

import com.hitbd.proj.Settings;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

public class TestModel {
    @Test
    public void testString2List() {
        UserC c = new UserC();
        c.setAuthedDevicesByText(null);
        Assert.assertEquals(0, c.getAuthedDevices().size());
        String input = "";
        c.setAuthedDevicesByText(input);
        Assert.assertEquals(0, c.getAuthedDevices().size());
        input = "123";
        c.setAuthedDevicesByText(input);
        Assert.assertEquals(1, c.getAuthedDevices().size());
        Assert.assertEquals(123L, c.getAuthedDevices().get(0));
        input = "123,456";
        c.setAuthedDevicesByText(input);
        Assert.assertEquals(2, c.getAuthedDevices().size());
        Assert.assertEquals(123L, c.getAuthedDevices().get(0));
        Assert.assertEquals(456L, c.getAuthedDevices().get(1));
    }

    @Test
    public void testList2String() {
        UserC c = new UserC();
        Assert.assertEquals("", c.getAuthedDevicesText());
        ArrayList<Long> list = new ArrayList<>();
        list.add(123L);
        c.setAuthedDevices(list);
        Assert.assertEquals("123", c.getAuthedDevicesText());
        list.add(456L);
        Assert.assertEquals("123,456", c.getAuthedDevicesText());
    }

    @Test
    public void testExpireDate() {
        Device device = new Device(1,1);
        device.setExpireListByText("");
        Assert.assertEquals(0, device.getExpireList().size());

        device.setExpireListByText("1,1");
        Assert.assertEquals(Settings.BASETIME + 1000L * 3600 * 24,
                device.getExpireList().get(0).getValue().getTime());

        device.setExpireListByText("1,1,2,2");
        Assert.assertEquals(Settings.BASETIME + 1000L * 3600 * 24,
                device.getExpireList().get(0).getValue().getTime());
        Assert.assertEquals(Settings.BASETIME + 1000L * 3600 * 24 * 2,
                device.getExpireList().get(1).getValue().getTime());
    }
}
