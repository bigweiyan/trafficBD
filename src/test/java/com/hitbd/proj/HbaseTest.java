package com.hitbd.proj;

import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

public class HbaseTest {
    @Ignore
    @Test
    public void testQuery() throws IOException, SQLException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("/usr/hbase-1.3.2.1/conf/hbase-site.xml");
        Settings.HBASE_CONFIG = configuration;
        Connection connection = ConnectionFactory.createConnection(Settings.HBASE_CONFIG);

        ArrayList<Integer> userBIds = new ArrayList<>();
        userBIds.add(2469);
        QueryFilter queryFilter = new QueryFilter();
        queryFilter.setAllowTimeRange(new Pair<>(new Date(1533225600000L), new Date(1533484800000L)));
        java.sql.Connection igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        Date date = new Date();
        AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(igniteConnection, 2469, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
        System.out.println(" total Time:" + (new Date().getTime() - date.getTime()) + "ms; create query " + scanner.queries.size());
        scanner.setConnection(connection);
        date = new Date();
        if (scanner.notFinished()) {
            scanner.next(50);
            System.out.println("Hbase response Time:" + (new Date().getTime() - date.getTime()) + "ms; scan alarm: " + scanner.getTotalAlarm());
        }
        while (scanner.notFinished()) {
            scanner.next(50);
        }
        System.out.println("Hbase total Time:" + (new Date().getTime() - date.getTime()) + "ms; scan alarm: " + scanner.getTotalAlarm());
    }
}
