package com.hitbd.proj.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.hitbd.proj.Settings;
import com.hitbd.proj.model.Pair;

public class Serialization {
	/**
	 * 测试序列转换函数
	 */
	public static void main(String[] args) {
		String[] lis = { "3423", "8743", "8723", "9279" };
		Date[] lli = { new Date(112, 0, 1), new Date(112, 1, 1),
				new Date(253, 0, 1), new Date(283, 0, 1) };
		countExpireList(lis, lli);
	}

	/**
	 * 布尔类型转int
	 * @param bool
	 * @return 转换后的int值
	 */
	public static int booToInt(boolean bool) {
		if (bool)
			return 1;
		else
			return 0;
	}

	/**
	 * list序列转字符串
	 * @param list 要转换的list
	 * @return 转换后的字符串
	 */
	public static <E> String listToStr(ArrayList<E> list) {
		int i;
		StringBuilder str = new StringBuilder();
		if (list.size() == 0)
			return "";
		for (E item : list) {
			str.append(item.toString()).append(",");
		}
		str.setLength(str.length()-1);
		return str.toString();
	}

	/**
	 * 字符串转list序列
	 * 
	 * @param str 要转换的字符串
	 * @return 转换后的list
	 */
	public static ArrayList<Integer> strToList(String str) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		if (str == null || str.equals(""))
			return list;
		String[] s = str.split(",");
		for (int i = 0; i < s.length; i++) {
			list.add(Integer.valueOf(s[i]));
		}
		return list;
	}

	public static ArrayList<Long> longToList(String str) {
		ArrayList<Long> list = new ArrayList<Long>();
		if (str == null || str.equals(""))
			return list;
		String[] s = str.split(",");
		for (int i = 0; i < s.length; i++) {
			list.add(Long.valueOf(s[i]));
		}
		return list;
	}

	/**
	 * 去掉user_B中父亲列表的孩子
	 * 
	 * @param children_ids child
	 */
	public static ArrayList<Integer> deleteChild(ArrayList<Integer> children_ids, int child) {
		children_ids.remove(Integer.valueOf(child));
		return children_ids;
	}

	/**
	 * 判断两个list是否还有公共值
	 * 
	 * @param list1
	 * @param list2
	 */
	public static boolean isContain(ArrayList<Long> list1, ArrayList<Long> list2) {
		for (int i = 0; i < list1.size(); i++) {
			for (int j = 0; j < list2.size(); j++) {
				if (list1.get(i).equals(list2.get(j)))
					return true;
			}
		}
		return false;
	}

	/**
	 * 求过期时间列表
	 * @param parentIds
     * @param expireDates parentIds 对应过期时间 expireDates
	 */
	public static String countExpireList(String[] parentIds, Date[] expireDates) {
		StringBuilder expirelist = new StringBuilder();
		int chick = 0;
		for (int i = 0; i < parentIds.length; i++) {
			long temp = (expireDates[i].getTime() - Settings.BASETIME) / (1000 * 60 * 60 * 24);
			if (temp < 0)
				return null;
			if (expireDates[i].compareTo(Settings.MAX_TIME) > 0) {
				temp = 0;
			} else
				temp = temp - chick;
			expirelist.append(parentIds[i]).append(",").append(temp).append(",");
			chick = (int) (chick + temp);
		}
		return expirelist.substring(0, expirelist.length() - 1);
	}

	public static List<Pair<Integer, Date>> getExpireList(String expireList) {
		if (expireList == null || expireList.isEmpty()) {
			return new ArrayList<>();
		}
		String[] list = expireList.split(",");
		if (list.length % 2 != 0) {
			throw new IllegalArgumentException("the argument should be even");
		}
		List<Pair<Integer, Date>> expireDate = new ArrayList<>();
		for (int i = 0; i < list.length; i+=2) {
			expireDate.add(new Pair<>(Integer.parseInt(list[i]),
					new Date(Settings.BASETIME + 1000L * Integer.parseInt(list[i + 1]) * 3600 * 24)));
		}
		return expireDate;
	}

}
