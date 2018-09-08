package com.hitbd.proj.logic;

import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

import java.util.List;
import java.util.Queue;

public class AlarmScanner {
    public Queue<Query> queries;
    private List<Pair<Integer, IAlarm>> alarms;
    private int currentPos;

    public List<Pair<Integer, IAlarm>> next() {
        return null;
    }
}
