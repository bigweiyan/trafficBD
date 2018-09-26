package com.hitbd.proj.logic;

import com.hitbd.proj.model.Pair;

import java.util.List;

public class Query {
    public String tableName;
    public String startRelativeSecond;
    public String endRelativeSecond;
    public List<Pair<Integer, Long>> imeis;
}
