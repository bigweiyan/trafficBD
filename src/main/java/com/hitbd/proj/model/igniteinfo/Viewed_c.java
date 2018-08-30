package com.hitbd.proj.model.igniteinfo;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class Viewed_c {
  public IgniteCache<Long, Integer> cache;
  public void createCache() {
    Ignition.setClientMode(true);
    Ignite ignite=Ignition.start();
    CacheConfiguration<Long, Integer> cfg = new CacheConfiguration<Long,Integer>();
    cfg.setName("Viewed_c");
    cfg.setCacheMode(CacheMode.PARTITIONED);//存储方式 PARTITIONED适合分布式存储
    cfg.setIndexedTypes(Long.class, Integer.class ); //必须设置索引类否则只能以key-value方式查询
    cache = ignite.getOrCreateCache(cfg);//根据配置创建缓存
  }
  public int getViewedCount(long imei) {
    return cache.get(imei);
  }
  public void setViewedCount(long imei, int count) {
    if(!cache.containsKey(imei)) {
      cache.put(imei, count);
    }
    else {
      cache.replace(imei, count);
    }
  }
  public static void main(String []args) {
    Viewed_c almc=new Viewed_c();
    almc.createCache();
    for(long i=0;i<8;i++) {
      almc.setViewedCount(i,(int) (i+88));
    }
    for(long i=0;i<8;i++) {
      System.out.println(almc.getViewedCount(i));
    }
  }
  
}
