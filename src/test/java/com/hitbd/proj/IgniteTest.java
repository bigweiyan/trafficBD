package com.hitbd.proj;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

public class IgniteTest {
    @Ignore
    @Test
    public void testCache() {
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

    @Ignore
    @Test
    public void testScanner() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        ArrayList<Integer> userBIds = new ArrayList<>();
        userBIds.add(2469);
        QueryFilter queryFilter = new QueryFilter();
        queryFilter.setAllowTimeRange(new Pair<>(new Date(1533225600000L), new Date(1533484800000L)));
        Date date = new Date();
        AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(connection, 2469, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
        System.out.println(" Use Time:" + (new Date().getTime() - date.getTime()) + "ms; create query " + scanner.queries.size());
    }
}
