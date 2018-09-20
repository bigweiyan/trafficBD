package com.hitbd.proj;

import com.hitbd.proj.model.Pair;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class QueryFilter {
    private Boolean allowReadStatus;
    private Set<String> allowAlarmStatus;
    private Set<String> allowAlarmType;
    private Pair<Date, Date> allowTimeRange;

    public Set<String> getAllowAlarmType() {
        return allowAlarmType;
    }
    public void setAllowAlarmType(Set<String> allowAlarmType) {
        this.allowAlarmType = allowAlarmType;
    }
    public Pair<Date, Date> getAllowTimeRange() {
        return allowTimeRange;
    }
    public void setAllowTimeRange(Pair<Date, Date> allowTimeRange) {
        this.allowTimeRange = allowTimeRange;
    }
    public Set<String> getAllowAlarmStatus() {
        return allowAlarmStatus;
    }
    public void setAllowAlarmStatus(Set<String> allowAlarmStatus) {
        this.allowAlarmStatus = allowAlarmStatus;
    }
    public Boolean getAllowReadStatus() {
        return allowReadStatus;
    }
    public void setAllowReadStatus(Boolean allowReadStatus) {
        this.allowReadStatus = allowReadStatus;
    }
}
