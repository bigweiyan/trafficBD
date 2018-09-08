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
    @Test
    public void testQuery() {
        ArrayList<Integer> userBIds = new ArrayList<>();
        userBIds.add(2469);
        QueryFilter queryFilter = new QueryFilter();
        queryFilter.setAllowTimeRange(new Pair<>(new Date(Settings.START_TIME), new Date(Settings.START_TIME + 1000)));
        AlarmScanner scanner = HbaseSearch.getInstance().queryAlarmByUser(2469, userBIds, true, HbaseSearch.NO_SORT, queryFilter);
        int i = 0;
        while (!scanner.queries.isEmpty()) {
            System.out.println("query: " + i);
            i++;
            Query q = scanner.queries.poll();
            System.out.println("table name: " + q.tableName);
            System.out.println("start time: " + q.startRelativeSecond);
            System.out.println("end time:" + q.endRelativeSecond);
            for (Pair<Integer, Long> pair : q.imeis) {
                System.out.println("user: " + pair.getKey() + " imei: " + pair.getValue());
            }
            System.out.println("===============================");
        }
    }
}
