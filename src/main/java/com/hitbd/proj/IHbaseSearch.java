package com.hitbd.proj;

import com.hitbd.proj.exception.ForeignKeyException;
import com.hitbd.proj.exception.NotExistException;
import com.hitbd.proj.exception.TimeException;
import com.hitbd.proj.logic.AlarmScanner;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * 本文件不允许擅自修改，有修改需求请联系负责人
 */
public interface IHbaseSearch {
    int NO_SORT = 0x00;
    int SORT_BY_CREATE_TIME = 0x01;
    int SORT_BY_USER_ID = 0x02;
    int SORT_BY_IMEI = 0x03;
    int SORT_BY_PUSH_TIME = 0x04;
    int SORT_ASC = 0x00;
    int SORT_DESC = 0x10;
    int FIELD_MASK = 0x0f;
    int ORDER_MASK = 0x10;
    /**
     * A5.1
     * 连接到Hbase集群
     * @return
     */
    boolean connect();

    /**
     * A5.1
     * 使用预定义的设置进行连接
     * @param config
     * @return
     */
    boolean connect(Configuration config);

    /**
     * A5.2
     * 获取一部分的告警数据，可以指定imei范围和时间范围
     * @param startImei
     * @param endImei
     * @param startTime
     * @param endTime
     * @return 告警数据列表
     */
    List<IAlarm> getAlarms(long startImei, long endImei, Date startTime, Date endTime);

    /**
     * No3.4
     * 新增告警操作
     * @param alarms 待插入的告警列表
     * @throws TimeException 告警的时间超过范围（小于2010年1月1日）时抛出此异常
     * @throws ForeignKeyException 告警的imei非法时抛出此异常
     */
    void insertAlarm(List<IAlarm> alarms) throws TimeException, ForeignKeyException;

    /**
     * No4.2
     * 设置推送时间
     * @param rowKeys 待设置的行键列表
     * @param pushTime
     * @throws NotExistException 行键不存在时抛出异常
     */
    void setPushTime(List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException;

    /**
     * No4.2
     * 设置已读标记
     * @param rowKeys 待设置的行键列表
     * @param viewed
     * @throws NotExistException 行键不存在时抛出异常
     */
    void setViewedFlag(List<Pair<String, String>> rowKeys, boolean viewed) throws NotExistException;

    /**
     * No4.5
     * 删除告警
     * @param rowKeys
     * @throws NotExistException
     */
    void deleteAlarm(List<Pair<String, String>> rowKeys) throws NotExistException;

    /**
     * 5.1-5.3a
     * 按照指定用户查询告警
     * @param userBIds 待查询的用户
     * @param recursive 是否递归查询其所有子用户
     * @param sortType 排序类型
     * @param filter 筛选类型
     * @return
     */
    AlarmScanner queryAlarmByUser(java.sql.Connection connection, int queryUser,
                                  List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter);

    /**
     * 5.1-5.3a
     * 按照指定设备查询告警
     * @param userAndDevices 待查询的设备
     * @param sortType 排序类型
     * @param filter 筛选类型
     * @return
     */
    AlarmScanner queryAlarmByImei(HashMap<Integer, List<Long>> userAndDevices, int sortType, QueryFilter filter);

    /**
     * 5.4
     * 按照用户C查询设备
     * @param userCId C端用户id
     * @param sortType 排序类型
     * @return
     */
    AlarmScanner queryAlarmByUserC(java.sql.Connection connection, int userCId, int sortType, QueryFilter filter);

    /**
     * 5.5
     * 按照imei和告警类型分组Count查询
     * @param parentBId
     * @param recursive 是否递归查询所有设备
     */
    Map<String, Integer> groupCountByImeiStatus(java.sql.Connection connection, int parentBId, boolean recursive);

    /**
     * 5.5 按照用户和已读标记分组Count查询
     * @param parentBIds
     * @param recursive 是否递归查询所有用户
     * @return
     */
    Map<String, Integer> groupCountByUserIdViewed(java.sql.Connection connection, ArrayList<Integer> parentBIds,
                                                  boolean recursive);

    /**
     * 5.5 按照用户Count查询，只要求前K个结果
     * @param parentBIds
     * @param recursive 是否递归查询所有用户
     * @param topK
     * @return
     */
    Map<Integer, Integer> groupCountByUserId(java.sql.Connection connection, ArrayList<Integer> parentBIds,
                                             boolean recursive, int topK);

    /**
     * 找到imei在一定时间范围内的全部告警数目
     * @param start String like mmdd
     * @param end String like mmdd
     * @param imeis 待查询的imei列表
     * @return imei与对应的告警计数
     */
    List<Pair<Long, Integer>> getAlarmCount(Connection connection, String start, String end, List<Long> imeis);

    /**
     * A5.3
     * 关闭连接
     * @return
     */
    boolean close();
}
