package com.hitbd.proj.model;

import java.util.List;

public interface IUserB {

    int getUserBId();

    void setUserBId(int id);

    /**
     * 获取父用户id，如果没有父用户返回-1
     * @return
     */
    int getParentId();

    /**
     * 设置父用户id，如果没有父用户，可以设置为-1
     * @param id
     */
    void setParentId(int id);

    List<Integer> getChildren();

    void setChildren(List<Integer> childrenIds);
}
