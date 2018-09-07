import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.HbaseSearch;
import com.hitbd.proj.Settings;
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
    public void testScan() throws IOException {
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.master", "192.168.1.134");
//        configuration.set("hbase.rootdir", "hdfs://host:9000/hbase");
//        configuration.set("hbase.cluster.distributed", "false");
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Table table = connection.getTable(TableName.valueOf("alarm_0730"));
//        Scan scan = new Scan("00862368010125258364d80".getBytes(), "00862368010125258364d89".getBytes());
//        scan.setBatch(100);
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result result = scanner.next(); result != null; result = scanner.next())
//            System.out.println("Found row : " + result);
//        scanner.close();
//        Get get = new Get("00862368010125258364d89".getBytes());
//        Result result = table.get(get);
//        System.out.println("get!");
//        System.out.println(new String(result.getValue("r".getBytes(), "type".getBytes())));
    }
}
