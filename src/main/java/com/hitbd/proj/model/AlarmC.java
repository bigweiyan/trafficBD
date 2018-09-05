package com.hitbd.proj.model;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class AlarmC {
	static private Ignite ignite;
	static private IgniteCache<Long, Integer> cache;

	/**
	 * 新建或获取alarmC表
	 */
	public void createCache() {
		//首先调用启动客户端的函数启动客户端
		
		ignite = Ignition.ignite(); 
		CacheConfiguration<Long, Integer> cfg = new CacheConfiguration<Long, Integer>();
		cfg.setName("alarm_c");
		cfg.setCacheMode(CacheMode.PARTITIONED);// 存储方式 PARTITIONED适合分布式存储
		cfg.setIndexedTypes(Long.class, Integer.class); // 必须设置索引类否则只能以key-value方式查询
		cache = ignite.getOrCreateCache(cfg);// 根据配置创建缓存
	}

	/**
	 * 获取设备对应的告警数
	 * @param imei
	 * @return
	 */
	public Integer getAlarmCount(long imei) {
		return cache.get(imei);
	}

	/**
	 * 插入设备对应的告警数
	 * @param imei
	 * @param count
	 */
	public void setAlarmCount(long imei, int count) {
		if (cache.containsKey(imei))
			cache.replace(imei, count);
		else
			cache.put(imei, count);
	}

}
