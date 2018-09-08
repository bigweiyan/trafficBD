package com.hitbd.proj;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class IgniteTest {
    @Ignore
    @Test
    public void testAlarmC() {
        /*
         * 测试AlarmC和ViewedC是否可以正确执行
         * 已经完成的测试，出于性能原因，在打包时进行忽略
         */
        IgniteSearch search = IgniteSearch.getInstance();
        search.setAlarmCount(123L, 2);
        Assert.assertEquals(search.getAlarmCount(123L), 2);
        search.setAlarmCount(456L, 456);
        Assert.assertEquals(search.getAlarmCount(456L), 456);
        search.setViewedCount(123L, 1);
        Assert.assertEquals(search.getViewedCount(123L), 1);
    }
}
