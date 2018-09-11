package com.hitbd.proj;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

public class HbaseTest {
    @Ignore
    @Test
    public void testQuery() throws IOException{
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("/usr/hbase-1.3.2.1/conf/hbase-site.xml");
        Settings.HBASE_CONFIG = configuration;
        ArrayList<Integer> userBIds = new ArrayList<>();
        userBIds.add(2469);
        QueryFilter queryFilter = new QueryFilter();
        queryFilter.setAllowTimeRange(new Pair<>(new Date(1533225600000L), new Date(1533484800000L)));
        Date date = new Date();
        AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(2469, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
        System.out.println(" Use Time:" + (new Date().getTime() - date.getTime()) + "ms; create query " + scanner.queries.size());
        date = new Date();
        while (!scanner.isFinished()) {
            scanner.next();
        }
        System.out.println("Hbase Use Time:" + (new Date().getTime() - date.getTime()) + "ms; scan alarm: " + scanner.getTotalAlarm());
    }
}
