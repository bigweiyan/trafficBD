package com.hitbd.proj.model;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class ViewedC {
	static private Ignite ignite;
	static private IgniteCache<Long, Integer> cache;
	/**
	 * 新建或获取cache
	 */
	public void createCache() {

		ignite = Ignition.ignite();
		CacheConfiguration<Long, Integer> cfg = new CacheConfiguration<Long, Integer>();
		cfg.setName("viewed_c");
		cfg.setCacheMode(CacheMode.PARTITIONED);// 存储方式 PARTITIONED适合分布式存储
		cfg.setIndexedTypes(Long.class, Integer.class); // 必须设置索引类否则只能以key-value方式查询
		cache = ignite.getOrCreateCache(cfg);// 根据配置创建缓存
	}
	/**
	 * 查找已读告警数
	 * @param imei
	 * @return
	 */
	public int getViewedCount(long imei) {
		return cache.get(imei);
	}
	/**
	 * 更新或插入已读告警数
	 * @param imei
	 * @param count
	 */
	public void setViewedCount(long imei, int count) {
		if (!cache.containsKey(imei)) {
			cache.put(imei, count);
		} else {
			cache.replace(imei, count);
		}
	}

}
