package com.hitbd.proj.model;

import java.util.ArrayList;
import java.util.List;

import com.hitbd.proj.model.IUserB;

public class UserB implements IUserB {
    private int user_id;
    public int parent_id;
    public String children_ids;

    public UserB(){};

    public UserB(int user_id, int parent_id) {
        super();
        this.user_id = user_id;
        this.parent_id = parent_id;
    }

    public UserB(int user_id, String children_ids) {
        super();
        this.user_id = user_id;
        this.children_ids = children_ids;
    }

    public UserB(int user_id, int parent_id, String children_ids) {
        super();
        this.user_id = user_id;
        this.children_ids = children_ids;
        this.parent_id = parent_id;
    }

    @Override
    public int getUserBId() {
        return this.user_id;
    }

    @Override
    public void setUserBId(int id) {
        this.user_id = id;
    }

    @Override
    public int getParentId() {
        return this.parent_id;
    }

    @Override
    public void setParentId(int id) {
        this.parent_id = id;
    }

    @Override
    public List<Integer> getChildren() {
        if (children_ids == null) return new ArrayList<>();
        String[] children = children_ids.split(",");
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 0; i < children.length; i++) {
            result.add(Integer.valueOf(children[i]));
        }
        return result;
    }

    @Override
    public void setChildren(List<Integer> childrenIds) {
        if (childrenIds == null) {
            this.children_ids = "";
            return;
        }
        StringBuilder temp = new StringBuilder();
        for (Integer child : childrenIds) {
            temp.append(child).append(",");
        }
        if (temp.length() > 0) temp.setLength(temp.length() - 1);
        this.children_ids = temp.toString();
    }
}
