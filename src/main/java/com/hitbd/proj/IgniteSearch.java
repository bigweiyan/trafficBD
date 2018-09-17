package com.hitbd.proj;

import com.hitbd.proj.Exception.DuplicatedPKException;
import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.*;
import com.hitbd.proj.model.UserC;
import com.hitbd.proj.util.Serialization;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class IgniteSearch implements IIgniteSearch {
    static Ignite ignite;
    static IgniteCache<Long, Integer> alarmCCache;
    static IgniteCache<Long, Integer> viewedCCache;
    Connection connection;
    static IgniteSearch search = null;
//	static {
//        Ignition.setClientMode(true);
//        ignite = Ignition.start();
//        CacheConfiguration<Long, Integer> cfg = new CacheConfiguration<Long, Integer>();
//        cfg.setName("alarm_c");
//        cfg.setCacheMode(CacheMode.PARTITIONED);// 存储方式 PARTITIONED适合分布式存储
//        cfg.setIndexedTypes(Long.class, Integer.class); // 必须设置索引类否则只能以key-value方式查询
//        alarmCCache = ignite.getOrCreateCache(cfg);// 根据配置创建缓存
//        cfg = new CacheConfiguration<Long, Integer>();
//        cfg.setName("viewed_c");
//        cfg.setCacheMode(CacheMode.PARTITIONED);// 存储方式 PARTITIONED适合分布式存储
//        cfg.setIndexedTypes(Long.class, Integer.class); // 必须设置索引类否则只能以key-value方式查询
//        viewedCCache = ignite.getOrCreateCache(cfg);// 根据配置创建缓存
//	}

    private IgniteSearch(){};

	public static IgniteSearch getInstance() {
	    if (search == null) {
	        search = new IgniteSearch();
        }
        return search;
    }

	@Override
	public boolean connect() {
		try {
			if(connection == null || connection.isClosed()) {
				connection = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean connect(String hostname, int port) {
		try {
			if(connection == null || connection.isClosed()) {
			connection = DriverManager.getConnection("jdbc:ignite:thin://" + hostname + ":" + port + "/");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public int getAlarmCount(long imei) {
		Integer result = alarmCCache.get(imei);
		return result == null ? 0 : result;
	}

	@Override
	public void setAlarmCount(long imei, int count) {
        if (alarmCCache.containsKey(imei))
            alarmCCache.replace(imei, count);
        else
            alarmCCache.put(imei, count);
	}

	@Override
	public int getViewedCount(long imei) {
        Integer result = viewedCCache.get(imei);
        return result == null ? 0 : result;
	}

	@Override
	public void setViewedCount(long imei, int count) {
        if (viewedCCache.containsKey(imei))
            viewedCCache.replace(imei, count);
        else
            viewedCCache.put(imei, count);
	}

	@Override
	public IUserB getUserB(int userBId) throws NotExistException {
		try(PreparedStatement pstmt = connection.prepareStatement("SELECT user_id FROM UserB WHERE user_id = ?")) {
			pstmt.setInt(1, userBId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.isBeforeFirst()) {
				int user_id = rs.getInt("user_id");
				int parent_id = rs.getInt("parent_id");
				String children_ids = rs.getString("children_ids");
				return new UserB(user_id, parent_id, children_ids);
			} else {
				throw new NotExistException();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public IDevice getDevice(int imei) throws NotExistException {
		try(PreparedStatement pstmt = connection.prepareStatement("SELECT imei FROM Device WHERE imei = ?")) {
			pstmt.setLong(1, imei);
			ResultSet rs = pstmt.executeQuery();
			if (rs.isBeforeFirst()) {
				int user_b_id = rs.getInt("user_b_id");
				long Imei = rs.getInt("imei");
				String device_type = rs.getString("device_type");
				String deviceName = rs.getString("device_name");
				String expireDate = rs.getString("expire_date");
				Device device = new Device(user_b_id, Imei, device_type, deviceName);
				if (expireDate != null && !expireDate.isEmpty()) {
					device.setExpireListByText(expireDate);
				}
				return device;
			} else {
				throw new NotExistException();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public IUserC getUserC(int userCId) throws NotExistException {
		try(PreparedStatement pstmt = connection.prepareStatement("SELECT user_id FROM UserC WHERE imei = ?")) {
			pstmt.setInt(1, userCId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.isBeforeFirst()) {
				int user_id = rs.getInt("user_id");
				String devices = rs.getString("devices");
				String authed_device = rs.getString("authed_device");
				String auth_user_ids = rs.getString("auth_user_ids");
				return new UserC(user_id, devices, authed_device, auth_user_ids);
			} else {
				throw new NotExistException();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int createUserB(int parentId) {
		try(PreparedStatement pstmt = connection.prepareStatement("SELECT user_id FROM UserB WHERE user_id = ?");
				PreparedStatement pstmt2 = connection.prepareStatement("SELECT user_id FROM UserB ")) {
			pstmt.setInt(1, parentId);
			ResultSet rs = pstmt.executeQuery();
			pstmt.setInt(1, parentId);
			ResultSet rs2 = pstmt2.executeQuery();
			rs2.last();
			int newid = rs2.getInt("user_id") + 1;
			if (rs.isBeforeFirst()) {
				UserB b = new UserB(newid, parentId);
				addUserBData(b, connection);
				return newid;
			} else {
				UserB b = new UserB(newid, -1);
				addUserBData(b, connection);
				return newid;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	private int addUserBData(UserB usr, Connection conn) throws SQLException {
		String sql = "insert into user_b values(?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, usr.getUserBId());
		pstmt.setInt(2, usr.getParentId());
		pstmt.setString(3, "");
		int result = pstmt.executeUpdate();
		pstmt.close();
		return result;
	}

	@Override
	public int createUserC() {
		int newid = -1;
		try (PreparedStatement pstmt = connection.prepareStatement("SELECT user_id FROM UserC ")){
			ResultSet rs = pstmt.executeQuery();
			rs.last();
			newid = rs.getInt("user_id") + 1;
			UserC c = new UserC(newid, null, null, null);
			addUserCData(c, connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return newid;
	}

	public int addUserCData(UserC usr, Connection conn) throws SQLException {
		String sql = "insert into UserC values(?,?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, usr.getUserCId());
		pstmt.setString(2, usr.getDevicesText());
		pstmt.setString(3, usr.getAuthedDevicesText());
		pstmt.setString(4, usr.getAuthUserIdsText());
		int result = pstmt.executeUpdate();
		pstmt.close();
		return result;
	}

	@Override
	public void createDevice(long imei, int userBId) throws DuplicatedPKException, ForeignKeyException {
		try (PreparedStatement pstmt = connection.prepareStatement("SELECT user_id FROM UserB WHERE user_id = ?");
				PreparedStatement pstmtimei = connection.prepareStatement("SELECT imei FROM Device WHERE imei = ?")){
			pstmt.setInt(1, userBId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.isBeforeFirst()) {
				pstmtimei.setLong(1, imei);
				ResultSet rsimei = pstmtimei.executeQuery();
				if (!rsimei.isBeforeFirst()) {
					String sql2 = "insert into Device(user_b_id,imei) values(?,?)";
					PreparedStatement stmt = connection.prepareStatement(sql2);
					stmt.setInt(1, userBId);
					stmt.setLong(2, imei);
					stmt.executeUpdate();
				} else {
					throw new DuplicatedPKException();
				}
			} else {
				throw new ForeignKeyException();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setNewParent(int childBId, int parentBId) throws ForeignKeyException, NotExistException {
		try {
			String sql = "select parent_id from user_b where user_id=?";
			PreparedStatement pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, childBId);
			ResultSet rs1 = pstmt.executeQuery();
			int parent = rs1.getInt("parent_id");
			if (parent == -1) {
				sql = "select user_id from user_b where user_id=?";
				pstmt = connection.prepareStatement(sql);
				pstmt.setInt(1, parentBId);
				ResultSet rs2 = pstmt.executeQuery();
				String parentChildren = rs2.getString("children_ids");
				if (rs2.isBeforeFirst()) {
					sql = "update user_b set parent_id=? where user_id=?";
					pstmt = connection.prepareStatement(sql);
					pstmt.setInt(1, parentBId);
					pstmt.setInt(2, childBId);
					pstmt.executeUpdate();
					sql = "update user_b set children_ids=? where user_id=?";
					pstmt = connection.prepareStatement(sql);
					pstmt.setString(1, parentChildren + "," + childBId);
					pstmt.setInt(2, parentBId);
					pstmt.executeUpdate();
					pstmt.close();
				} else {
					pstmt.close();
					throw new NotExistException();
				}
			} else {
				pstmt.close();
				throw new ForeignKeyException();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void addUserCDevice(int userCId, long imei) throws ForeignKeyException, NotExistException {

	}

	@Override
	public void authorizeCDevice(long imei, int toCId) throws ForeignKeyException, NotExistException {

	}

	@Override
	public void updateDevice(long imei, String deviceType, String deviceName, String projectId, boolean enabled,
			boolean repayment, String isupdate) throws SQLException {
		String sql = "update Device set ";
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
		PreparedStatement pstmt = connection.prepareStatement(sql);
		pstmt.executeUpdate();
		pstmt.close();

	}

	@Override
	public void deleteParentLink(int childBId) throws SQLException, NotExistException {
		String sql = "select parent_id from user_b where user_id = " + String.valueOf(childBId);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		int parent = -1;
		if (rs.next()) {
			parent = rs.getInt("parent_id");
			if (parent == -1) {
				throw new NotExistException();
			}
			// delete child
			sql = "select children_ids from UserB where user_id = " + String.valueOf(parent);
			pstmt = connection.prepareStatement(sql);
			rs = pstmt.executeQuery();
			String child_ids = null;
			if (rs.next())
				child_ids = rs.getString("children_ids");
			String sql2 = "update UserB set children_ids = '"
					+ Serialization.listToStr(Serialization.deleteChild(Serialization.strToList(child_ids), childBId))
					+ "' where user_id = " + String.valueOf(parent);
			pstmt = connection.prepareStatement(sql2);
			pstmt.executeUpdate();
			// delete parent
			sql = "update UserB set parent_id=null  where user_id=" + String.valueOf(childBId);
			pstmt = connection.prepareStatement(sql);
			pstmt.executeUpdate();
			// delete time
			ArrayList<Integer> childqueue = new ArrayList<Integer>();
			ArrayList<Long> imeis = new ArrayList<Long>();
			int count = 1;
			childqueue.add(childBId);
			while (!childqueue.isEmpty()) {
				// query new value
				sql = "select imei from Device where user_b_id in (" + Serialization.listToStr(childqueue) + ")";
				sql2 = "select children_ids from UserB where user_id in (" + Serialization.listToStr(childqueue) + ")";

				pstmt = connection.prepareStatement(sql);
				ResultSet imeiset = pstmt.executeQuery();
				pstmt = connection.prepareStatement(sql2);
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
				sql = "select imei,expire_list from Device where imei in (" + Serialization.listToStr(imeis) + ")";
				pstmt = connection.prepareStatement(sql);
				ResultSet expireset = pstmt.executeQuery();
				connection.setAutoCommit(false);
				sql = "update Device set expire_list = ? where imei= ?";
				pstmt = connection.prepareStatement(sql);
				pstmt.clearBatch();
				while (expireset.next()) {
					ArrayList<Integer> temp = Serialization.strToList(expireset.getString("expire_list"));
					StringBuilder newvalue = new StringBuilder();
					for (int i = 0; i <= count * 2 - 1; i++) {
						newvalue.append(Integer.toString(temp.get(i))).append(",");
					}
					pstmt.setString(1, newvalue.substring(0, newvalue.length() - 1));
					pstmt.setLong(2, expireset.getLong("imei"));
					pstmt.addBatch();
				}
				pstmt.executeBatch();
				connection.setAutoCommit(true);
				count++;
			}
			// Ignite.disConnect(conn);
			pstmt.close();
		}
	}

	@Override
	public void relocateDevice(long imei, int toBid, String[] parentIds, Date[] expireDates)
			throws SQLException, TimeException {
		// is toBid exist
		String sql = "select * from user_b where user_id = " + String.valueOf(toBid);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		pstmt.executeQuery();
		// count expire_list
		String expire_list = Serialization.countExpireList(parentIds, expireDates);
		if (expire_list == null) // 时间格式出错
			throw new TimeException();
		// update
		sql = "update Device set user_b_id=?,expire_list=? where imei=?";
		pstmt = connection.prepareStatement(sql);
		pstmt.setInt(1, toBid);
		pstmt.setString(2, expire_list);
		pstmt.setLong(3, imei);
		pstmt.executeUpdate();
		pstmt.close();

	}

	@Override
	public void deleteDevice(long imei) throws SQLException {
		int usrid;
		// find user_c
		String sql = "select user_c_id from Device where imei=" + Long.toString(imei);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next())
			usrid = rs.getInt("user_c_id");
		else {
			pstmt.close();
			rs.close();
			return;
		}
		// delete imei
		sql = "delete from Device where imei=" + Long.toString(imei);
		pstmt = connection.prepareStatement(sql);
		pstmt.execute();
		// clear Device list
		sql = "select devices,auth_user_ids from UserC where user_id=" + Integer.toString(usrid);
		pstmt = connection.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices;
		ArrayList<Integer> aud;
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			devices.remove(imei);
			String temp = rs1.getString("auth_user_ids");
			aud = Serialization.strToList(temp);
			// aud exist
			if (aud.size() > 0) {
				sql = "select user_id,authed_device from user_c where user_id in (" + temp + ")";
				pstmt = connection.prepareStatement(sql);
				ResultSet rs2 = pstmt.executeQuery();

				connection.setAutoCommit(false);
				sql = "update user_c set authed_device = ? where user_id = ?";
				pstmt = connection.prepareStatement(sql);
				pstmt.clearBatch();
				while (rs2.next()) {
					ArrayList<Long> adevice = Serialization.longToList(rs2.getString("authed_device"));
					int user = rs2.getInt("user_id");
					if (adevice.contains(imei)) {
						adevice.remove(imei);
						if (!Serialization.isContain(devices, adevice))
							aud.remove(Integer.valueOf(user));
						pstmt.setString(1, Serialization.listToStr(adevice));
						pstmt.setInt(2, user);
						pstmt.addBatch();
					}
				}
				pstmt.executeBatch();
				sql = "update UserC set auth_user_ids='" + Serialization.listToStr(aud) + "', devices='"
						+ Serialization.listToStr(devices) + "' where user_id = " + Integer.toString(usrid);
				PreparedStatement pstmt1 = connection.prepareStatement(sql);
				pstmt1.executeUpdate();
				connection.commit();
				connection.setAutoCommit(true);
				pstmt.close();
				rs.close();
				rs1.close();
				rs2.close();
			}

		} else {
			pstmt.close();
			rs.close();
			rs1.close();
		}

	}

	@Override
	public void deleteAuthorization(long imei, int userCId) throws SQLException {
		// update authed list
		String sql = "select authed_device from user_c where user_id = " + Integer.toString(userCId);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<Long> adevice = new ArrayList<>();
		if (rs.next())
			adevice = Serialization.longToList(rs.getString("authed_device"));
		adevice.remove(imei);
		String sql1 = "update user_c  set authed_device='" + Serialization.listToStr(adevice) + "' where user_id="
				+ Integer.toString(userCId);

		// update auth_user_ids
		// sql="select user_c_id from Device where imei="+Long.toString(imei);
		sql = "select devices,auth_user_ids from user_c where user_id = (select "
				+ "user_c_id from Device where imei = " + Long.toString(imei) + ")";
		pstmt = connection.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices = new ArrayList<>();
		ArrayList<Integer> aui = new ArrayList<>();
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			aui = Serialization.strToList(rs1.getString("auth_user_ids"));
		}
		connection.setAutoCommit(false);
		pstmt = connection.prepareStatement(sql1);
		pstmt.executeUpdate();
		PreparedStatement pstmt1 = null;
		if (!Serialization.isContain(devices, adevice)) {
			aui.remove(Integer.valueOf(userCId));
			sql = "update user_c set auth_user_ids = '" + Serialization.listToStr(aui) + "' where "
					+ "user_id = (select user_c_id from Device where imei = " + Long.toString(imei) + ")";
			pstmt1 = connection.prepareStatement(sql);
			pstmt1.executeUpdate();
		}
		connection.commit();
		connection.setAutoCommit(true);
		pstmt.close();
		rs.close();
		rs1.close();
		if (pstmt1 != null)
			pstmt1.close();

	}

	@Override
	public void updateExpireDate(long imei, int userBId, Date expireDate) throws TimeException, NotExistException {
		try {
			long day = (expireDate.getTime() - Settings.BASETIME) / (24 * 60 * 60 * 1000);
			if (day < 0) { // 日期不合法
				throw new TimeException();
			}
			String sql = "select expire_list from Device where imei = " + imei;
			PreparedStatement pstmt = connection.prepareStatement(sql);
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
					if (expireDate.compareTo(Settings.MAXTIME) > 0)
						time = 0;
					else
						time = (int) (day - list.get(i));
				}
			}
			if (i == list.size()) { // 不是该设备的父用户
				throw new NotExistException();
			}

			sql = "update Device set expire_list = '" + Serialization.listToStr(list) + "' where imei = " + imei;
			pstmt = connection.prepareStatement(sql);
			pstmt.executeUpdate();
			pstmt.close();
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void removeCDevice(long imei) throws SQLException {
		String sql = "select user_c_id from Device where imei = " + String.valueOf(imei);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		int usrid = -1;
		if (rs.next())
			usrid = rs.getInt("user_c_id");
		// update imei
		sql = "update Device set user_c_id = -1 where imei=" + Long.toString(imei);
		pstmt = connection.prepareStatement(sql);
		pstmt.execute();
		// clear Device list
		sql = "select devices,auth_user_ids from UserC where user_id=" + Integer.toString(usrid);
		pstmt = connection.prepareStatement(sql);
		ResultSet rs1 = pstmt.executeQuery();
		ArrayList<Long> devices;
		ArrayList<Integer> aud;
		if (rs1.next()) {
			devices = Serialization.longToList(rs1.getString("devices"));
			devices.remove(imei);
			String temp = rs1.getString("auth_user_ids");
			aud = Serialization.strToList(temp);
			// aud exist
			if (aud.size() > 0) {
				sql = "select user_id,authed_device from user_c where user_id in (" + temp + ")";
				pstmt = connection.prepareStatement(sql);
				ResultSet rs2 = pstmt.executeQuery();

				connection.setAutoCommit(false);
				sql = "update user_c set authed_device = ? where user_id = ?";
				pstmt = connection.prepareStatement(sql);
				pstmt.clearBatch();
				while (rs2.next()) {
					ArrayList<Long> adevice = Serialization.longToList(rs2.getString("authed_device"));
					int user = rs2.getInt("user_id");
					if (adevice.contains(imei)) {
						adevice.remove(imei);
						if (!Serialization.isContain(devices, adevice))
							aud.remove(Integer.valueOf(user));
						pstmt.setString(1, Serialization.listToStr(adevice));
						pstmt.setInt(2, user);
						pstmt.addBatch();
					}
				}
				pstmt.executeBatch();
				sql = "update UserC set auth_user_ids='" + Serialization.listToStr(aud) + "', devices='"
						+ Serialization.listToStr(devices) + "' where user_id = " + Integer.toString(usrid);
				PreparedStatement pstmt1 = connection.prepareStatement(sql);
				pstmt1.executeUpdate();
				connection.commit();
				connection.setAutoCommit(true);
				pstmt.close();
				rs1.close();
				rs2.close();
			}

		} else {
			pstmt.close();
			rs1.close();
		}
	}

	@Override
	public boolean close() {
		if (connection == null) {
			return false;
		}
		try {
			connection.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

    /**
     * 补充代码1
     * 根据用户id查找直接设备
     * @param userBId 被查询用户
     * @param relativeId 查询用户（必须是userBID的父用户）
     * @param useExpire 是否使用过期时间对设备进行过滤
     * @return 直属设备列表
     */
    public List<Long> getDirectDevices(int userBId, int relativeId, boolean useExpire) {
        if (connection == null) connect();
        List<Long> result = new ArrayList<>();
        Date now = new Date();
        try (PreparedStatement pstmt = connection.prepareStatement("SELECT imei, expire_list FROM Device WHERE user_b_id = ?;")){
            pstmt.setInt(1, userBId);
            ResultSet rs=pstmt.executeQuery();
            while(rs.next()){
                long imei = rs.getLong("imei");
                if (useExpire) {
                    String expireListText = rs.getString("expire_list");
                    List<Pair<Integer, Date>> expireList = Serialization.getExpireList(expireListText);
                    for (Pair<Integer, Date> pair : expireList) {
                        if (pair.getKey() == relativeId) {
                            Date date = pair.getValue();
                            if (date.after(now)) {
                                result.add(imei);
                            }
                            break;
                        }
                    }
                }else {
                    result.add(imei);
                }
            }
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 补充代码2
     * 获取一个B端用户的所有孩子
     * @param userBId
     * @return 孩子的bid列表
     */
    public List<Integer> getChildren(int userBId) {
        if (connection == null) connect();
        List<Integer> children = new ArrayList<>();
        try (PreparedStatement pst = connection.prepareStatement("SELECT children_ids from user_b where user_id = ?;")){
            pst.setInt(1, userBId);
            ResultSet resultSet = pst.executeQuery();
            while (resultSet.next()) {
                String childrenText = resultSet.getString("children_ids");
                children.addAll(Serialization.strToList(childrenText));
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return children;
    }

    public HashMap<Integer, List<Long>> getChildrenDevicesOfUserB(int userBId){
        return getChildrenDevicesOfUserB(userBId, true);
    }

    /**
     * 补充代码3
     * 获取用户的所有可访问设备
     * @param userBId 查询用户
	 * @param useExpire 如果为true，则过滤掉过期设备
     * @return
     */
    public HashMap<Integer, List<Long>> getChildrenDevicesOfUserB(int userBId, boolean useExpire){
        HashMap<Integer, List<Long>> userImeiMap = new HashMap<>();
        Queue<Integer> queue = new LinkedList<>();
        queue.offer(userBId);
        while (!queue.isEmpty()) {
            int user = queue.poll();
            List<Integer> children_list = getChildren(user);
            for (Integer element : children_list) {
                queue.offer(element);
            }

            List<Long> directDevices = getDirectDevices(user, userBId, useExpire);
            userImeiMap.put(user, directDevices);
        }
        return userImeiMap;
    }
    /**
     * 补充代码4
     * 层序遍历获取用户的所有可访问设备
     * @param userBId 查询用户
	 * @param useExpire 如果为true，则过滤掉过期设备
     * @return
     * @throws SQLException 
     */
    public HashMap<Integer, List<Long>> getLevelOrderChildrenDevicesOfUserB(int userBId, boolean useExpire){
        HashMap<Integer, List<Long>> userImeiMap = new HashMap<>();
		ArrayList<Integer> childqueue = new ArrayList<Integer>();
		ArrayList<Long> imeis = new ArrayList<Long>();
		PreparedStatement pstmt = null;
		childqueue.add(userBId);
		try {
            while (!childqueue.isEmpty()) {
                // query new value
                String sql = "select user_b_id,imei from Device where user_b_id in (" + Serialization.listToStr(childqueue) + ")";
                pstmt = connection.prepareStatement(sql);
                ResultSet imeiset = pstmt.executeQuery();
                sql = "select children_ids from User_B where user_id in (" + Serialization.listToStr(childqueue) + ")";
                pstmt = connection.prepareStatement(sql);
                ResultSet childrenset = pstmt.executeQuery();
                // refresh imeis and childqueue
                imeis.clear();
                childqueue.clear();
                while (imeiset.next()) {
                    long imei = imeiset.getLong("imei");
                    int userid = imeiset.getInt("user_b_id");
                    if (userImeiMap.containsKey(userid)) {
                        userImeiMap.get(userid).add(imei);
                    } else {
                        List<Long> list = new ArrayList<>();
                        list.add(imei);
                        userImeiMap.put(userid, list);
                    }
                }
                while (childrenset.next()) {
                    ArrayList<Integer> temp = Serialization.strToList(childrenset.getString("children_ids"));
                    for (int i = 0; i < temp.size(); i++) {
                        childqueue.add(temp.get(i));
                    }
                }
            }
            pstmt.close();
        }catch (SQLException e){
		    e.printStackTrace();
        }
        return userImeiMap;
    }
    
}
