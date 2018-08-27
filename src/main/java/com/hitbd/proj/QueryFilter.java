package com.hitbd.proj;

import com.hitbd.proj.model.Pair;

import java.util.Date;
import java.util.List;

public class QueryFilter {
    public List<Integer> getAllowUserIds() {
        return allowUserIds;
    }

    public void setAllowUserIds(List<Integer> allowUserIds) {
        this.allowUserIds = allowUserIds;
    }

    public List<Long> getAllowIMEIs() {
        return allowIMEIs;
    }

    public void setAllowIMEIs(List<Long> allowIMEIs) {
        this.allowIMEIs = allowIMEIs;
    }

    public List<String> getAllowAlarmType() {
        return allowAlarmType;
    }

    public void setAllowAlarmType(List<String> allowAlarmType) {
        this.allowAlarmType = allowAlarmType;
    }

    public Pair<Date, Date> getAllowTimeRange() {
        return allowTimeRange;
    }

    public void setAllowTimeRange(Pair<Date, Date> allowTimeRange) {
        this.allowTimeRange = allowTimeRange;
    }

    private List<Integer> allowUserIds;
    private List<Long> allowIMEIs;
    private List<String> allowAlarmType;
    private Pair<Date, Date> allowTimeRange;
}
