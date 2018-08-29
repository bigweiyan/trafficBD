package com.hitbd.proj;

import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class HbaseSearch implements IHbaseSearch {
    @Override
    public boolean connect() {
        return false;
    }

    @Override
    public boolean connect(Configuration config) {
        return false;
    }

    @Override
    public List<IAlarm> getAlarms(long startImei, long endImei, Date startTime, Date endTime) {
        return null;
    }

    @Override
    public void insertAlarm(List<IAlarm> alarms) throws TimeException, ForeignKeyException {

    }

    @Override
    public void setPushTime(List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException {

    }

    @Override
    public void setViewedFlag(List<Pair<String, String>> rowKeys, boolean viewed) throws NotExistException {

    }

    @Override
    public void deleteAlarm(List<Pair<String, String>> rowKeys) throws NotExistException {

    }

    @Override
    public List<IAlarm> queryAlarmByUser(List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        return null;
    }

    @Override
    public List<IAlarm> queryAlarmByImei(List<Long> imeis, int sortType, QueryFilter filter) {
        return null;
    }

    @Override
    public void asyncQueryAlarmByUser(int qid, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {

    }

    @Override
    public void asyncQueryAlarmByImei(int qid, List<Long> imeis, int sortType, QueryFilter filter) {

    }

    @Override
    public List<IAlarm> queryAlarmByUserC(int userCId, int sortType) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByImeiStatus(int parentBId, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserIdViewed(List<Integer> parentBIds, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserId(List<Integer> parentBIds, boolean recursive, int topK) {
        return null;
    }

    @Override
    public boolean close() {
        return false;
    }
}
