package com.hitbd.proj.logic;

import com.hitbd.proj.Settings;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class AlarmScanner {
    // 未来可以优化为生产者-消费者模式
    public Queue<Query> queries;
    private List<Pair<Integer, IAlarm>> alarms;
    private int currentThreads;
    private boolean ready;

    public AlarmScanner() {
        currentThreads = 0;
        alarms = new ArrayList<>();
        ready = false;
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
    }

    public synchronized List<Pair<Integer, IAlarm>> next() {
        if (queries.isEmpty()) return null;
        while (!ready){
            if (currentThreads == 0) {
                int startThread = 0;
                for (int i = 0; i < Settings.MAX_THREAD; i ++){
                    Query q = queries.poll();
                    if (q == null) break;
                    startThread ++;
                    new ScanThread(q).start();
                }
                currentThreads = startThread;
            }
        }
        return null;
    }

    synchronized void putAlarm(List<Pair<Integer, IAlarm>> newAlarms) {
        currentThreads --;
        alarms.addAll(newAlarms);
        if (currentThreads == 0) ready = true;
        this.notify();
    }

    class ScanThread extends Thread{
        private Query query;
        public ScanThread(Query query){
            this.query = query;
        }
        @Override
        public void run() {

        }
    }

}

