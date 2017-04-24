package com.netease.backend.coordinator.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;

import com.mysql.fabric.xmlrpc.base.Array;
import com.netease.backend.coordinator.config.CoordinatorConfig;
import com.netease.backend.coordinator.log.Checkpoint;
import com.netease.backend.coordinator.log.LogRecord;
import com.netease.backend.coordinator.log.LogScanner;
import com.netease.backend.coordinator.log.LogType;
import com.netease.backend.coordinator.log.db.LogScannerImp;
import com.netease.backend.coordinator.monitor.MonitorException;
import com.netease.backend.coordinator.monitor.MonitorRecord;
import com.netease.backend.coordinator.transaction.Transaction;
import com.netease.backend.tcc.common.Action;
import com.netease.backend.tcc.common.HeuristicsInfo;
import com.netease.backend.tcc.common.IllegalActionException;
import com.netease.backend.tcc.common.LogException;
import com.netease.backend.tcc.error.CoordinatorException;
import com.netease.backend.tcc.error.HeuristicsException;

public class DbUtil {
	private BasicDataSource localDataSource = null;
	private BasicDataSource systemDataSource = null;
	private static int PARTITION_NUM = 14;
	private CoordinatorConfig config = null;
	
	public DbUtil(CoordinatorConfig config, BasicDataSource localDataSource, 
			BasicDataSource systemDataSource) throws CoordinatorException {
		this.config = config;
		this.localDataSource = localDataSource;
		this.systemDataSource = systemDataSource;
		initLocalDb();
	}
	
	public BasicDataSource getLocalDataSource() {
		return localDataSource;
	}

	public void setLocalDataSource(BasicDataSource localDataSource) {
		this.localDataSource = localDataSource;
	}

	public BasicDataSource getSystemDataSource() {
		return systemDataSource;
	}

	public void setSystemDataSource(BasicDataSource systemDataSource) {
		this.systemDataSource = systemDataSource;
	}

	/**
	 * Description: get serverId from local or system database 
	 * @return serverId
	 * @throws CoordinatorException
	 */
	public int getServerId() throws CoordinatorException {
		int serverId = -1;
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		ResultSet localRset = null;
		// read local database to fetch serverId
		try {
			localConn = localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("SELECT SERVER_ID FROM COORDINATOR_INFO");
			localRset = localPstmt.executeQuery();
			if (localRset.next()) {
				serverId = localRset.getInt(1);
				return serverId;
			}
		} catch (SQLException e) {
			throw new CoordinatorException("Cannot fetch local ServerId", e);
		} finally {
			try {
				if (localRset != null)
					localRset.close();
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		
		// if local serverId not exist means the node is not initiated
		// register this node to system database and get a unique serverId
		// if have no server id, take one from system db
		Connection sysConn = null;
		PreparedStatement sysPstmt = null;
		ResultSet sysRset = null;
		try {
			sysConn = systemDataSource.getConnection();
			sysPstmt = sysConn.prepareStatement("SELECT SERVER_ID FROM SERVER_INFO WHERE SERVER_IP = ? AND RDS_IP = ?");
			
			sysPstmt.setString(1, config.getServerIp());
			sysPstmt.setString(2, config.getRdsIp());
			sysRset = sysPstmt.executeQuery();
			if (sysRset.next()) {
				serverId = sysRset.getInt(1);
			} else {
				// if not exist in system db, then alloc one 
				sysRset.close();
				sysPstmt.close();
				
				sysPstmt = sysConn.prepareStatement("Insert into SERVER_INFO(SERVER_IP, RDS_IP) values (?, ?)", Statement.RETURN_GENERATED_KEYS);
				
				sysPstmt.setString(1, config.getServerIp());
				sysPstmt.setString(2, config.getRdsIp());
				
				sysPstmt.executeUpdate();
				sysRset = sysPstmt.getGeneratedKeys();
				if (sysRset.next())
					serverId = sysRset.getInt(1);
				else
					throw new CoordinatorException("Cannot get a coordinator id by inserting");
			}
		} catch (SQLException e) {
			throw new CoordinatorException("Cannot get a new ServerId", e);
		} finally {
			try {
				if (sysRset != null)
					sysRset.close();
				if (sysPstmt != null)
					sysPstmt.close();
				if (sysConn != null)
					sysConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		
		// insert new ServerId to local DB
		try {
			localConn = localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("INSERT INTO COORDINATOR_INFO(SERVER_ID, CHECKPOINT, MAX_UUID) VALUES(?, ?, ?)");
			// set update value
			localPstmt.setInt(1, serverId);
			localPstmt.setLong(2, 0);
			localPstmt.setLong(3, 0);
			localPstmt.executeUpdate();
		} catch (SQLException e) {
			throw new CoordinatorException("Cannot update local ServerId", e);
		} finally {
			try {
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		
	
		return serverId;
	}

	/**
	 * Description: Write a log to local database
	 * @param tx
	 * @param logType
	 * @throws LogException
	 */
	public void writeLog(Transaction tx, LogType logType) throws LogException {
		byte[] trxContent = null;
		long trxTimestamp = 0;
		switch(logType) {
		case TRX_BEGIN:
			trxContent = LogUtil.serialize(tx.getExpireList());
			trxTimestamp = tx.getCreateTime();
			break;
		case TRX_START_EXPIRE:
			trxContent = LogUtil.serialize(tx.getExpireList());
			trxTimestamp = tx.getBeginTime();
			break;
		case TRX_START_CONFIRM:
			trxContent = LogUtil.serialize(tx.getConfirmList());
			trxTimestamp = tx.getBeginTime();
			break;
		case TRX_START_CANCEL:
			trxContent = LogUtil.serialize(tx.getCancelList());
			trxTimestamp = tx.getBeginTime();
			break;
		default:
			trxTimestamp = tx.getEndTime();
			trxContent = null;	
		}
		
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		
		try {
			localConn = localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("INSERT INTO COORDINATOR_LOG(TRX_ID, TRX_STATUS, TRX_TIMESTAMP, TRX_CONTENT) VALUES(?,?,?,?)");
			
			// set insert values
			localPstmt.setLong(1, tx.getUUID());
			localPstmt.setInt(2, logType.getCode());
			localPstmt.setLong(3, trxTimestamp);
			localPstmt.setBytes(4, trxContent);
			
			localPstmt.executeUpdate();
		} catch (SQLException e) {
			throw new LogException("Write log error", e);
		} finally {
			try {
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Description: Check expire is valid
	 * @param uuid
	 * @return true if expire is valid
	 * @throws LogException
	 */
	public boolean checkExpire(long uuid) throws LogException {
		Connection systemConn = null;
		PreparedStatement systemPstmt = null;
		ResultSet systemRset = null;
		int res = 0;
		
		// insert record to Expire_trx_info to avoid other node to confirm/cancel
		try {
			systemConn = systemDataSource.getConnection();
			systemPstmt = systemConn.prepareStatement("INSERT IGNORE INTO EXPIRE_TRX_INFO(TRX_ID, TRX_ACTION, TRX_TIMESTAMP)" +
					" VALUES(?,?,?)");
			
			systemPstmt.setLong(1, uuid);
			systemPstmt.setInt(2, Action.EXPIRE.getCode());
			systemPstmt.setLong(3, System.currentTimeMillis());
			systemPstmt.executeUpdate();
			res = systemPstmt.getUpdateCount();
		} catch (SQLException e) {
			throw new LogException("Check expire error", e);
		} finally {
			try {
				if (systemPstmt != null)
					systemPstmt.close();
				if (systemConn != null)
					systemConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		

		// must duplicate key, then check the action
		// if the action is confirm/cancel return invalid
		if (res == 0) {
			try {
				systemConn = systemDataSource.getConnection();
				systemPstmt = systemConn.prepareStatement("SELECT TRX_ACTION FROM EXPIRE_TRX_INFO WHERE TRX_ID = ?");
				systemPstmt.setLong(1, uuid);
				systemRset = systemPstmt.executeQuery();
				// if other node confirm or cancel this trx , then checkfailed
				if (systemRset.next()) {
					if (systemRset.getInt(1) != Action.EXPIRE.getCode()) {
						return false;
					} else { 
						return true;
					}
				} else {
					throw new RuntimeException("the expire record disappeared, why?");
				}
			} catch (SQLException e) {
				throw new LogException("Check expire error", e);
			} finally {
				try {
					if (systemRset != null)
						systemRset.close();
					if (systemPstmt != null)
						systemPstmt.close();
					if (systemConn != null)
						systemConn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
		return true;
	}

	/**
	 * Description: set checkpoint to local database
	 * @param checkpoint
	 * @throws LogException
	 */
	public void setCheckpoint(Checkpoint checkpoint) throws LogException {
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		
		try {
			localConn = this.localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("UPDATE COORDINATOR_INFO SET CHECKPOINT = ?, MAX_UUID = ?");
			
			// set insert values
			localPstmt.setLong(1, checkpoint.getTimestamp());
			localPstmt.setLong(2, checkpoint.getMaxUuid());
			
			localPstmt.executeUpdate();
		} catch (SQLException e) {
			throw new LogException("Update checkpoint error", e);
		} finally {
			try {
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}

	/**
	 * Description: get checkpoint from local database
	 * @return
	 * @throws LogException
	 */
	public Checkpoint getCheckpoint() throws LogException {
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		ResultSet localRset = null;
		long timestamp = 0;
		long maxUuid = 0;
		try {
			localConn = this.localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("SELECT CHECKPOINT, MAX_UUID FROM COORDINATOR_INFO");
			
			localRset = localPstmt.executeQuery();
			if (localRset.next()) {
				timestamp = localRset.getLong(1);
				maxUuid = localRset.getLong(2);
			} else {
				throw new LogException("No checkpoint record found in local db");
			}
		} catch (SQLException e) {
			throw new LogException("Read checkpoint error", e);
		} finally {
			try {
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return new Checkpoint(timestamp, maxUuid);
	}

	/**
	 * Description: read log from checkpoint
	 * @param startpoint
	 * @return
	 * @throws LogException
	 */
	public LogScanner beginScan(long startpoint) throws LogException {
		try {
			Connection conn = this.localDataSource.getConnection();
			PreparedStatement pstmt = conn.prepareStatement("SELECT TRX_ID, TRX_STATUS, TRX_TIMESTAMP, TRX_CONTENT FROM COORDINATOR_LOG WHERE TRX_TIMESTAMP >= ?", 
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			pstmt.setLong(1, startpoint);
			pstmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rset = pstmt.executeQuery();
			return new LogScannerImp(this, conn, pstmt, rset);
		} catch (SQLException e) {
			throw new LogException("Start read log error", e);
		} 
	}
	
	/**
	 * Description: determine whether having more log record
	 * @param rset
	 * @return true if log has next record
	 * @throws LogException
	 */
	public boolean hasNext(ResultSet rset) throws LogException {
		try {
			return rset.next();
		} catch (SQLException e) {
			throw new LogException("Read log has next error", e);
		}
	}

	/**
	 * Description: get next log record
	 * @param rset
	 * @return log record
	 * @throws LogException
	 */
	public LogRecord getNextLog(ResultSet rset) throws LogException {
		try {
			long uuid = rset.getLong("TRX_ID");
			LogType logType = LogType.values()[rset.getInt("TRX_STATUS")];
			long timestamp = rset.getLong("TRX_TIMESTAMP");
			byte[] procs = rset.getBytes("TRX_CONTENT");
			return new LogRecord(uuid, logType, timestamp, procs);
		} catch (SQLException e) {
			throw new LogException("Read next log error", e);
		}	
	}

	/**
	 * Description: end scan and destroy resultset,
	 * 				preparedstatement and connection
	 * @param conn
	 * @param pstmt
	 * @param rset
	 * @throws LogException
	 */
	public void endScan(Connection conn, PreparedStatement pstmt,
			ResultSet rset) throws LogException {
		try {
			if (rset != null)
				rset.close();
			if (pstmt != null)
				pstmt.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			throw new LogException("Ene read log error", e);
		}
	}

	/**
	 * Description: check confirm/cancel in recovery is valid
	 * @param uuid
	 * @return true if action is valid
	 * @throws LogException
	 * @throws IllegalActionException 
	 */
	public void checkRetryAction(long uuid, Action action) throws LogException, IllegalActionException {
		Connection systemConn = null;
		PreparedStatement systemPstmt = null;
		ResultSet systemRset = null;
		
		int res = 0;
		try {
			// Insert a record to system db, and mark trx action
			systemConn = this.systemDataSource.getConnection();
			systemPstmt = systemConn.prepareStatement("INSERT IGNORE INTO EXPIRE_TRX_INFO(TRX_ID, TRX_ACTION, TRX_TIMESTAMP)" +
					" VALUES(?,?,?)");
			
			systemPstmt.setLong(1, uuid);
			systemPstmt.setInt(2, action.getCode());
			systemPstmt.setLong(3, System.currentTimeMillis());
			
			systemPstmt.executeUpdate();
			res = systemPstmt.getUpdateCount();
		} catch (SQLException e) {
			throw new LogException("Check retry action error", e);
		} finally {
			try {
				if (systemPstmt != null)
					systemPstmt.close();
				if (systemConn != null)
					systemConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		

		// must duplicate key, then check the action
		// if the table record's action is expired, return invalid 
		if (res == 0) {
			try {
				systemConn = this.systemDataSource.getConnection();
				systemPstmt = systemConn.prepareStatement("SELECT TRX_ACTION FROM EXPIRE_TRX_INFO WHERE TRX_ID = ?");
				systemPstmt.setLong(1, uuid);
				systemRset = systemPstmt.executeQuery();
				if (systemRset.next()) {
					// if other node expire this trx , then checkfailed
					int actionCode = systemRset.getInt(1);
					if (actionCode != action.getCode()) {
						throw new IllegalActionException(uuid, Action.getAction(actionCode), action);
					}
				} else {
					throw new RuntimeException("action record disappear from sysdb, why?");
				}
			} catch (SQLException e) {
				throw new LogException("Check retry action error", e);
			} finally {
				try {
					if (systemRset != null)
						systemRset.close();
					if (systemPstmt != null)
						systemPstmt.close();
					if (systemConn != null)
						systemConn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

	/**
	 * Description: select 1 to check RDS is alive
	 * @return true if rds is alive
	 */
	public boolean checkLocaLogMgrAlive() {
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		ResultSet localRset = null;
		try {
			localConn = this.localDataSource.getConnection();
			localPstmt = localConn.prepareStatement("SELECT 1");
			
			
			localRset = localPstmt.executeQuery();
			
			if (localRset.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			return false;
		} finally {
			try {
				if (localRset != null)
					localRset.close();
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Description: write heuristic record in system db or local db
	 * @param tx
	 * @param action
	 * @param e
	 * @param isLocal  if true write record to local db, else to system db
	 * @throws LogException
	 */
	public void writeHeuristicRec(Transaction tx, Action action,
			HeuristicsException e, boolean isLocal) throws LogException {
		BasicDataSource dataSource = isLocal ? this.localDataSource : this.systemDataSource;
		Connection conn = null;
		PreparedStatement pstmt = null;
		byte[] trxContent = null;
		switch(action) {
		case EXPIRE:
			trxContent = LogUtil.serialize(tx.getExpireList());
			break;
		case CONFIRM:
			trxContent = LogUtil.serialize(tx.getConfirmList());
			break;
		case CANCEL:
			trxContent = LogUtil.serialize(tx.getCancelList());
			break;
		default:
			trxContent = null;	
		}
		
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement("INSERT IGNORE INTO HEURISTIC_TRX_INFO(TRX_ID, TRX_ACTION, TRX_HEURISTIC_CODE, TRX_TIMESTAMP, TRX_CONTENT) VALUES(?,?,?,?,?)");
			pstmt.setLong(1, tx.getUUID());
			pstmt.setInt(2, action.getCode());
			pstmt.setShort(3, e.getCode());
			pstmt.setLong(4, tx.getLastTimeStamp());
			pstmt.setBytes(5, trxContent);
			
			pstmt.executeUpdate();
		} catch (SQLException e1) {
			throw new LogException("Write heuristic record error", e1);
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e1) {
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * Description: write monitor data to system database
	 * @param rec
	 * @throws MonitorException
	 */
	public void writeMonitorRec(MonitorRecord rec) throws MonitorException {
		Connection systemConn = null;
		PreparedStatement systemPstmt = null;
		
		try {
			systemConn = systemDataSource.getConnection();
			systemPstmt = systemConn.prepareStatement("INSERT INTO SERVER_MONITOR" +
					"(SERVER_ID, TIMESTAMP, CUR_TRX_NUM, CUR_PROCESS_TRX_NUM," +
					" REGISTED_TRX_NUM, CONFIRM_NUM, CANCEL_NUM, EXPIRE_NUM," +
					" AVG_REGISTED_TIME, MAX_REGISTED_TIME, " +
					"AVG_CONFIRM_TIME, MAX_CONFIRM_TIME, AVG_CANCEL_TIME, MAX_CANCEL_TIME)" +
					" values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)" );
			
			systemPstmt.setInt(1,  rec.getServerId());
			systemPstmt.setLong(2, rec.getTimestamp());
			systemPstmt.setLong(3, rec.getCurTrxNum());
			systemPstmt.setLong(4, rec.getCurProcessTrxNum());
			systemPstmt.setLong(5, rec.getRegistTrxNum());
			systemPstmt.setLong(6, rec.getConfirmTrxNum());
			systemPstmt.setLong(7, rec.getCancelTrxNum());
			systemPstmt.setLong(8, rec.getExpireTrxNum());
			systemPstmt.setLong(9, rec.getAvgRegistTime());
			systemPstmt.setLong(10, rec.getMaxRegistTime());
			systemPstmt.setLong(11, rec.getAvgConfirmTime());
			systemPstmt.setLong(12, rec.getMaxConfirmTime());
			systemPstmt.setLong(13, rec.getAvgCancelTime());
			systemPstmt.setLong(14, rec.getMaxCancelTime());
			
			systemPstmt.executeUpdate();
		} catch (SQLException e) {
			throw new MonitorException("Write monitor record error", e);
		} finally {
			try {
				if (systemPstmt != null)
					systemPstmt.close();
				if (systemConn != null)
					systemConn.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * Description: check and init local database
	 * @throws CoordinatorException
	 */
	public void initLocalDb() throws CoordinatorException {
		// check whether local database is already inited
		Connection localConn = null;
		PreparedStatement localPstmt = null;
		ResultSet localRset = null;
		DatabaseMetaData localDbmd = null;
		
		try {
			localConn = localDataSource.getConnection();
			localDbmd = localConn.getMetaData();
			localRset = localDbmd.getTables("tcc", null, 
					"COORDINATOR_LOG", new String[]{"TABLE"});
			if (localRset.next()) {
				return;
			}
		} catch (SQLException e) {
			throw new CoordinatorException("Read local database error", e);
		}  finally {
			try {
				if (localRset != null)
					localRset.close();
				if (localConn != null)
					localConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		// Create table
		String createCoordinatorInfoTableSql = "CREATE TABLE `COORDINATOR_INFO` (" + 
				"`SERVER_ID` int(11) NOT NULL," +
				"`CHECKPOINT` bigint(20) NOT NULL," +
				"`MAX_UUID` bigint(20) NOT NULL," +
				"PRIMARY KEY (`SERVER_ID`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		String createHeuristicInfoTableSql = "CREATE TABLE `HEURISTIC_TRX_INFO` (" +
				"`TRX_ID` bigint(20) NOT NULL," +
				"`TRX_ACTION` tinyint(4) NOT NULL," +
				"`TRX_HEURISTIC_CODE` smallint(6) NOT NULL," +
				"`TRX_TIMESTAMP` bigint(20) NOT NULL," +
				"`TRX_CONTENT` varbinary(4096) DEFAULT NULL," +
				"PRIMARY KEY (`TRX_ID`)," +
				"KEY `IDX_TIME_STAMP` (`TRX_TIMESTAMP`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		// get  date to specify partition info, partition number is 14 day by default
		Calendar date = Calendar.getInstance();
		date.set(Calendar.HOUR, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.MILLISECOND, 0);
		date.add(Calendar.DATE, 1);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		String createCoordinatorLogTableSql = "CREATE TABLE `COORDINATOR_LOG` (" +
				"`LOG_ID` bigint(20) NOT NULL AUTO_INCREMENT," +
				"`TRX_ID` bigint(20) NOT NULL," +
				"`TRX_STATUS` tinyint(4) NOT NULL," +
				"`TRX_TIMESTAMP` bigint(20) NOT NULL," +
				"`TRX_CONTENT` varbinary(4096) DEFAULT NULL," +
				"PRIMARY KEY (`LOG_ID`,`TRX_TIMESTAMP`)," +
				"KEY `IDX_TIMESTAMP` (`TRX_TIMESTAMP`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8 " +
				"PARTITION BY RANGE(`TRX_TIMESTAMP`)(";
		
		String[] dateStrings = new String[PARTITION_NUM];
		long[] millTimes = new long[PARTITION_NUM];
		for (int i = 0; i < PARTITION_NUM;  i++) {
			dateStrings[i] = sdf.format(date.getTime());
			millTimes[i] = date.getTimeInMillis();
			date.add(Calendar.DATE, 1);
			createCoordinatorLogTableSql += "   PARTITION p" +
												dateStrings[i] +
												" VALUES LESS THAN (" +
												millTimes[i] +
												") ENGINE = InnoDB";
			if (i != PARTITION_NUM - 1) {
				createCoordinatorLogTableSql += ", "; 
			} else {
				createCoordinatorLogTableSql += ")";
			}
		}
		
		try {
			localConn = localDataSource.getConnection();
			localPstmt = localConn.prepareStatement(createCoordinatorInfoTableSql);
			localPstmt.executeUpdate();
			localPstmt.close();
			localPstmt = localConn.prepareStatement(createHeuristicInfoTableSql);
			localPstmt.executeUpdate();
			localPstmt.close();
			localPstmt = localConn.prepareStatement(createCoordinatorLogTableSql);
			localPstmt.executeUpdate();
		} catch (SQLException e) {
			throw new CoordinatorException("Init local database error", e);
		} finally {
			try {
				if (localPstmt != null)
					localPstmt.close();
				if (localConn != null)
					localPstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Description: get HeuristicsException info within given time bucket
	 * @param startTime
	 * @param endTime
	 * @return HeuristicsInfo list
	 * @throws LogException 
	 */
	public List<HeuristicsInfo> getHeuristicsExceptionList(long startTime, long endTime) 
			throws LogException {
		List<HeuristicsInfo> heuristicsInfos = new ArrayList<HeuristicsInfo>();
		Connection systemConn = null;
		PreparedStatement systemPstmt = null;
		ResultSet systemRset = null;
		try {
			systemConn = systemDataSource.getConnection();
			systemPstmt = systemConn.prepareStatement("SELECT TRX_ID, TRX_ACTION FROM HEURISTIC_TRX_INFO WHERE TRX_TIMESTAMP >= ? and TRX_TIMESTAMP < ?");
			systemPstmt.setLong(1, startTime);
			systemPstmt.setLong(2, endTime);
			systemRset = systemPstmt.executeQuery();
			while (systemRset.next()) {
				heuristicsInfos.add(new HeuristicsInfo(systemRset.getLong(1), 
						Action.getAction(systemRset.getInt(2))));
			}
			return heuristicsInfos;
		} catch (SQLException e) {
			throw new LogException("Cannot fetch system HeuristicsInfo", e);
		} finally {
			try {
				if (systemRset != null)
					systemRset.close();
				if (systemPstmt != null)
					systemPstmt.close();
				if (systemConn != null)
					systemConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
	}
	
	/**
	 * Description: remove HeuristicsExceptions from systemDB
	 * @param txId list
	 * @throws LogException 
	 */
	public void removeHeuristicsExceptions(List<Long> txIdList) 
			throws LogException{
		Connection systemConn = null;
		PreparedStatement systemPstmt = null;
		try {
			systemConn = systemDataSource.getConnection();
			systemConn.setAutoCommit(false);
			systemPstmt = systemConn.prepareStatement("DELETE FROM HEURISTIC_TRX_INFO WHERE TRX_ID = ?");
			for (Long txId : txIdList) {
				systemPstmt.setLong(1, txId);
				systemPstmt.addBatch();
			}
			systemPstmt.executeBatch();
			systemConn.commit();
		} catch (SQLException e) {
			throw new LogException("Cannot remove HeuristicsInfos form systemDB", e);
		} finally {
			try {
				if (systemPstmt != null)
					systemPstmt.close();
				if (systemConn != null)
					systemConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
	}
	
}
