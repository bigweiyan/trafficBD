package main.java.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import main.java.tool.Serialization;

public class IgniteSearch implements IIgniteSearch {

	private static Connection conn;

	/**
	 * No4.1 修改设备属性
	 * 
	 * @param imei
	 * @param deviceType
	 * @param deviceName
	 * @param projectId
	 * @param enabled
	 * @param repayment
	 * @throws NotExistException
	 *             该设备不存在时抛出此异常 如果设备不存在那么抛出sql一场
	 */
	@Override
	public void updateDevice(long imei, String deviceType, String deviceName, String projectId, boolean enabled,
			boolean repayment, String isupdate) throws SQLException {
		String sql = "update device set ";
		int i;
		String[] values = { deviceType, deviceName, projectId, String.valueOf(enabled), String.valueOf(repayment) };
		String[] properties = { "devicetype", "devicename", "projectid", "enabled", "repayment" };
		ArrayList<Integer> cl = Serialization.strToList(isupdate);
		for (i = 0; i < 5; i++) {
			if (cl.get(i).equals(Integer.valueOf(1)))
				sql = sql + properties[i] + "='" + values[i] + "',";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + " where imei=" + String.valueOf(imei);
		// System.out.println(sql);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();
		pstmt.close();

	}

	/**
	 * No4.3 删除父子关系
	 * 
	 * @param childBId
	 *            被删除的父子关系中的儿子
	 * @throws NotExistException
	 *             该id不存在或该id没有父用户时抛出异常
	 */
	@Override
	public void deleteParentLink(int childBId) throws SQLException {
		String sql = "select parent_id from user_b where user_id = " + String.valueOf(childBId);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		int parent = -1;
		if (rs.next())
			parent = rs.getInt("parent_id");
		else {
			// delete child
			sql = "select children_ids from user_B where user_id = " + String.valueOf(parent);
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			String child_ids = null;
			if (rs.next())
				child_ids = rs.getString("children_ids");
			String sql2 = "update user_B set children_ids = '"
					+ Serialization.listToStr(Serialization.deleteChild(Serialization.strToList(child_ids), childBId))
					+ "' where user_id = " + String.valueOf(parent);
			pstmt = conn.prepareStatement(sql2);
			pstmt.executeUpdate();
			// delete parent
			sql = "update user_B set parent_id=null  where user_id=" + String.valueOf(childBId);
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			// delete time
			ArrayList<Integer> childqueue = new ArrayList<Integer>();
			ArrayList<Long> imeis = new ArrayList<Long>();
			int count = 1;
			childqueue.add(childBId);
			while (childqueue.isEmpty() == false) {
				// query new value
				sql = "select imei from device where user_b_id in (" + Serialization.listToStr(childqueue) + ")";
				sql2 = "select children_ids from user_B where user_id in (" + Serialization.listToStr(childqueue) + ")";

				pstmt = conn.prepareStatement(sql);
				ResultSet imeiset = pstmt.executeQuery();
				pstmt = conn.prepareStatement(sql2);
				ResultSet childrenset = pstmt.executeQuery();
				// refresh imeis and childqueue
				imeis.clear();
				childqueue.clear();
				while (imeiset.next()) {
					imeis.add(imeiset.getLong("imei"));
				}
				while (childrenset.next()) {
					ArrayList<Integer> temp = Serialization.strToList(childrenset.getString("children_ids"));
					for (int i = 0; i < temp.size(); i++) {
						childqueue.add(temp.get(i));
					}
				}
				// delete
				sql = "select imei,expire_list from device where imei in (" + Serialization.listToStr(imeis) + ")";
				pstmt = conn.prepareStatement(sql);
				ResultSet expireset = pstmt.executeQuery();
				conn.setAutoCommit(false);
				sql = "update device set expire_list = ? where imei= ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.clearBatch();
				while (expireset.next()) {
					ArrayList<Integer> temp = Serialization.strToList(expireset.getString("expire_list"));
					String newvalue = new String();
					for (int i = 0; i <= count * 2 - 1; i++) {
						newvalue = newvalue + Integer.toString(temp.get(i)) + ",";
					}
					pstmt.setString(1, newvalue.substring(0, newvalue.length() - 1));
					pstmt.setLong(2, expireset.getLong("imei"));
					pstmt.addBatch();
				}
				pstmt.executeBatch();
				conn.setAutoCommit(true);
				count++;
			}
			// Ignite.disConnect(conn);
			pstmt.close();
		}
	}

	/**
	 * No4.4 将一个用户的设备转移给另一个用户
	 * 
	 * @param imei
	 *            被转移的设备
	 * @param toBid
	 *            被转移到的用户
	 * @throws NotExistException
	 *             设备或用户不存在时抛出异常
	 */
	@Override
	public static void relocateDevice(long imei, int toBid, String[] parentIds,Date[] expireDates) throws SQLException {
		// is toBid exist
		String sql = "select * from user_b where user_id = " + String.valueOf(toBid);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeQuery();
		//count expire_list
		String expire_list = Serialization.countExpireList(parentIds, expireDates);
		if(expire_list==null) //时间格式出错
			return;
		// update
		sql = "update device set user_b_id=?,expire_list=? where imei=?";
		pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, toBid);
		pstmt.setString(2, expire_list);
		pstmt.setLong(3, imei);
		pstmt.executeUpdate();
		pstmt.close();

	}

	/**
	 * No4.6 删除一个设备
	 * 
	 * @param imei
	 * @throws NotExistException
	 *             设备不存在时抛出异常
	 */
	@Override
	public void deleteDevice(long imei) throws SQLException {
		int usrid;
		// find user_c
		String sql = "select user_c_id from device where imei=" + Long.toString(imei);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next())
			usrid = rs.getInt("user_c_id");
		else {
			pstmt.close();
			rs.close();
			return;
		}
		// delete imei
		sql = "delete from device where imei=" + Long.toString(imei);
		pstmt = conn.prepareStatement(sql);
		pstmt.execute();
		// clear device list
		sql = "select devices,auth_user_ids from user_C where user_id=" + Integer.toString(usrid);
		pstmt = conn.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices;
		ArrayList<Integer> aud;
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			devices.remove(Long.valueOf(imei));
			String temp = rs1.getString("auth_user_ids");
			aud = Serialization.strToList(temp);
			// aud exist
			if (aud.size() > 0) {
				sql = "select user_id,authed_device from user_c where user_id in (" + temp + ")";
				pstmt = conn.prepareStatement(sql);
				ResultSet rs2 = pstmt.executeQuery();

				conn.setAutoCommit(false);
				sql = "update user_c set authed_device = ? where user_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.clearBatch();
				while (rs2.next()) {
					ArrayList<Long> adevice = Serialization.longToList(rs2.getString("authed_device"));
					int user = rs2.getInt("user_id");
					if (adevice.contains(imei)) {
						adevice.remove(Long.valueOf(imei));
						if (Serialization.isContain(devices, adevice) == false)
							aud.remove(Integer.valueOf(user));
						pstmt.setString(1, Serialization.listToStr(adevice));
						pstmt.setInt(2, user);
						pstmt.addBatch();
					}
				}
				pstmt.executeBatch();
				sql = "update user_C set auth_user_ids='" + Serialization.listToStr(aud) + "', devices='"
						+ Serialization.listToStr(devices) + "' where user_id = " + Integer.toString(usrid);
				PreparedStatement pstmt1 = conn.prepareStatement(sql);
				pstmt1.executeUpdate();
				conn.commit();
				conn.setAutoCommit(true);
				pstmt.close();
				rs.close();
				rs1.close();
				rs2.close();
				return;
			}

		} else {
			pstmt.close();
			rs.close();
			rs1.close();
			return;
		}

	}

	/**
	 * No4.7 删除一个用户的设备授权，但不会删除这个设备
	 * 
	 * @param imei
	 *            被移除授权的设备
	 * @param userCId
	 *            被移除授权的用户
	 * @throws SQLException
	 * @throws NotExistException
	 *             用户或设备不存在时抛出异常
	 */
	@Override
	public void deleteAuthorization(long imei, int userCId) throws SQLException {
		// update authed list
		String sql = "select authed_device from user_c where user_id = " + Integer.toString(userCId);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<Long> adevice = new ArrayList<>();
		if (rs.next())
			adevice = Serialization.longToList(rs.getString("authed_device"));
		adevice.remove(Long.valueOf(imei));
		String sql1 = "update user_c  set authed_device='" + Serialization.listToStr(adevice) + "' where user_id="
				+ Integer.toString(userCId);

		// update auth_user_ids
		// sql="select user_c_id from device where imei="+Long.toString(imei);
		sql = "select devices,auth_user_ids from user_c where user_id = (select "
				+ "user_c_id from device where imei = " + Long.toString(imei) + ")";
		pstmt = conn.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices = new ArrayList<>();
		ArrayList<Integer> aui = new ArrayList<>();
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			aui = Serialization.strToList(rs1.getString("auth_user_ids"));
		}
		conn.setAutoCommit(false);
		pstmt = conn.prepareStatement(sql1);
		pstmt.executeUpdate();
		PreparedStatement pstmt1 = null;
		if (Serialization.isContain(devices, adevice) == false) {
			aui.remove(Integer.valueOf(userCId));
			sql = "update user_c set auth_user_ids = '" + Serialization.listToStr(aui) + "' where "
					+ "user_id = (select user_c_id from device where imei = " + Long.toString(imei) + ")";
			pstmt1 = conn.prepareStatement(sql);
			pstmt1.executeUpdate();
		}
		conn.commit();
		conn.setAutoCommit(true);
		pstmt.close();
		rs.close();
		rs1.close();
		pstmt1.close();

	}

	/**
	 * No4.8 修改一个设备关于它某个父用户的过期时间
	 * 
	 * @param imei
	 *            设备号
	 * @param userBId
	 *            这个设备的一个父用户
	 * @param expireDate
	 *            新过期时间
	 * @throws NotExistException
	 *             用户或设备不存在，或该设备不是该用户的父用户时抛出异常
	 * @throws TimeException
	 *             时间不合法（早于2010年1月1日时抛出异常）
	 */
	@Override
	public static void updateExpireDate(long imei, int userBId, Date expireDate) throws SQLException {
		long day = (expireDate.getTime() - BASETIME.getTime()) / (24 * 60 * 60 * 1000);
		if(day<0) { //日期不合法
			return;
		}
		String sql = "select expire_list from device where imei = " + Long.valueOf(imei);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<Integer> list = new ArrayList<>();
		if (rs.next())
			list = Serialization.strToList(rs.getString("expire_list"));
		int time = -1;
		int i;
		for (i = 0; i < list.size(); i++) {
			if (i % 2 == 0 && list.get(i) == userBId) {
				list.set(i + 1, time);
				break;
			}
			if (i % 2 == 1) {
				if (expireDate.compareTo(MAXTIME)>0)
					time = 0;
				else
					time = (int) (day - list.get(i));
			}
		}
		if (i == list.size()) { //不是该设备的父用户
			return;
		}
		
		sql = "update device set expire_list = '" + Serialization.listToStr(list) + "' where imei = "
				+ Long.valueOf(imei);
		pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();
		pstmt.close();
		rs.close();
	}
	/**
	 * 4.10 移除一个设备与其C端用户的拥有关系，移除时不会删除该C端用户或设备
	 * 
	 * @param imei
	 * @throws NotExistException
	 *             用户或设备不存在时抛出异常
	 */
	@Override
	public void removeCDevice(long imei) throws SQLException {
		String sql = "select user_c_id from device where imei = " + String.valueOf(imei);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		int usrid = -1;
		if (rs.next())
			usrid = rs.getInt("user_c_id");
		// update imei
		sql = "update device set user_c_id = -1 where imei=" + Long.toString(imei);
		pstmt = conn.prepareStatement(sql);
		pstmt.execute();
		// clear device list
		sql = "select devices,auth_user_ids from user_C where user_id=" + Integer.toString(usrid);
		pstmt = conn.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices;
		ArrayList<Integer> aud;
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			devices.remove(Long.valueOf(imei));
			String temp = rs1.getString("auth_user_ids");
			aud = Serialization.strToList(temp);
			// aud exist
			if (aud.size() > 0) {
				sql = "select user_id,authed_device from user_c where user_id in (" + temp + ")";
				pstmt = conn.prepareStatement(sql);
				ResultSet rs2 = pstmt.executeQuery();

				conn.setAutoCommit(false);
				sql = "update user_c set authed_device = ? where user_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.clearBatch();
				while (rs2.next()) {
					ArrayList<Long> adevice = Serialization.longToList(rs2.getString("authed_device"));
					int user = rs2.getInt("user_id");
					if (adevice.contains(imei)) {
						adevice.remove(Long.valueOf(imei));
						if (Serialization.isContain(devices, adevice) == false)
							aud.remove(Integer.valueOf(user));
						pstmt.setString(1, Serialization.listToStr(adevice));
						pstmt.setInt(2, user);
						pstmt.addBatch();
					}
				}
				pstmt.executeBatch();
				sql = "update user_C set auth_user_ids='" + Serialization.listToStr(aud) + "', devices='"
						+ Serialization.listToStr(devices) + "' where user_id = " + Integer.toString(usrid);
				PreparedStatement pstmt1 = conn.prepareStatement(sql);
				pstmt1.executeUpdate();
				conn.commit();
				conn.setAutoCommit(true);
				pstmt.close();
				rs1.close();
				rs2.close();
				return;
			}

		} else {
			pstmt.close();
			rs1.close();
			return;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
