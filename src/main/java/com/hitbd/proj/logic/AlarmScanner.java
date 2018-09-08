package com.hitbd.proj.logic;

import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.List;

public class AlarmScanner {
    ResultScanner scanner;

    public List<Pair<Integer, IAlarm>> next() {
        return null;
    }
}
