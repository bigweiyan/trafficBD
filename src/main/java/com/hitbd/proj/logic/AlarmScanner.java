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
    private boolean finished;

    public AlarmScanner() {
        currentThreads = 0;
        alarms = new ArrayList<>();
        ready = false;
        finished = false;
    }

    public void setQueries(Queue<Query> queries) {
        this.queries = queries;
        if (queries == null || queries.isEmpty()) {
            finished = true;
        }
    }

    public synchronized List<Pair<Integer, IAlarm>> next() {
        if (finished) return null;
        while (!ready){
            // 线程为0，未准备好，表示是第一次执行next方法。此时进行第一次查询，创建查询线程
            if (currentThreads == 0) {
                int startThread = 0;
                for (int i = 0; i < Settings.MAX_THREAD; i ++){
                    Query q = queries.poll();
                    if (q == null) {
                        // 已经完成查询，下次调用方法会直接返回null
                        finished = true;
                        break;
                    }
                    startThread ++;
                    new ScanThread(q).start();
                }
                currentThreads = startThread;
            }else {
                // 线程不为0，表示正在查询，需要等待
                try {
                    this.wait(40);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
        // 将结果保存在临时变量，并将类变量换一个新数组
        List<Pair<Integer, IAlarm>> result = alarms;
        alarms = new ArrayList<>();

        // 如果此时空闲，可以开始为下次方法调用做准备
        if (currentThreads == 0) {
            int startThread = 0;
            for (int i = 0; i < Settings.MAX_THREAD; i ++){
                Query q = queries.poll();
                if (q == null) {
                    // 想开启新任务，却没有查询可以开启时，认为查询已经结束
                    if (startThread == 0) finished = true;
                    break;
                }
                ready = false;
                startThread ++;
                new ScanThread(q).start();
            }
            currentThreads = startThread;
        }
        return result;
    }

    synchronized void putAlarm(List<Pair<Integer, IAlarm>> newAlarms) {
        currentThreads --;
        alarms.addAll(newAlarms);
        // 这是最后一个工作中的线程，表示数据已经准备好
        if (currentThreads == 0) ready = true;
        this.notify();
    }

    public boolean isFinished() {
        return finished;
    }

    class ScanThread extends Thread{
        private Query query;
        public ScanThread(Query query){
            this.query = query;
        }
        @Override
        public void run() {
            List<Pair<Integer, IAlarm>> result = new ArrayList<>();
            // TODO 查询的代码写在这儿
            for (Pair<Integer, Long> pair:query.imeis) {
                System.out.println(this.getName() + ": " + pair.getValue());
            }
            AlarmScanner.this.putAlarm(result);
        }
    }

}

