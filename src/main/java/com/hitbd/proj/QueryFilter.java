package com.hitbd.proj;

import com.hitbd.proj.model.Pair;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class QueryFilter {

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

    private Set<String> allowAlarmType;
    private Pair<Date, Date> allowTimeRange;
}
