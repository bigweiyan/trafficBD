package com.hitbd.proj.model;

import java.io.Serializable;

public class Pair<K,V> implements Serializable{

    private K key;

    public K getKey() { return key; }

    private V value;

    public V getValue() { return value; }

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        return key + "=" + value;
    }

    public int hashCode() {
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Pair) {
            Pair pair = (Pair) o;
            if (key != null ? !key.equals(pair.key) : pair.key != null) return false;
            if (value != null ? !value.equals(pair.value) : pair.value != null) return false;
            return true;
        }
        return false;
    }

    public void setValue(V value) {
        this.value = value;
    }
}

