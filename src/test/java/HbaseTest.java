import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.QueryFilter;
import com.hitbd.proj.Settings;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.logic.Query;
import com.hitbd.proj.model.AlarmImpl;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.*;

import java.io.IOException;
import java.util.*;

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
        System.out.println("Ignite Use Time:" + (new Date().getTime() - date.getTime()) + "ms; create query " + scanner.queries.size());
//        int i = 0;
//        while (!scanner.queries.isEmpty()) {
//            System.out.println("query: " + i);
//            i++;
//            Query q = scanner.queries.poll();
//            System.out.println("table name: " + q.tableName);
//            System.out.println("start time: " + q.startRelativeSecond);
//            System.out.println("end time:" + q.endRelativeSecond);
//            for (Pair<Integer, Long> pair : q.imeis) {
//                System.out.println("user: " + pair.getKey() + " imei: " + pair.getValue());
//            }
//            System.out.println("===============================");
//        }
        date = new Date();
        while (!scanner.isFinished()) {
            scanner.next();
        }
        System.out.println("Hbase Use Time:" + (new Date().getTime() - date.getTime()) + "ms");
    }
}
