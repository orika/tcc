package com.mogujie.tcc.coordinator.log.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.mogujie.tcc.coordinator.log.LogRecord;
import com.mogujie.tcc.coordinator.util.DbUtil;
import com.mogujie.tcc.coordinator.log.LogScanner;
import com.mogujie.tcc.common.LogException;

public class LogScannerImp implements LogScanner{
	private Connection conn;
	private PreparedStatement pstmt;
	private ResultSet rset;
	private DbUtil dbUtil;
	
	public LogScannerImp(DbUtil dbUtil, Connection conn, PreparedStatement pstmt,
			ResultSet rset) {
		super();
		this.dbUtil = dbUtil;
		this.conn = conn;
		this.pstmt = pstmt;
		this.rset = rset;
	}

	public void setDbUtil(DbUtil dbUtil) {
		this.dbUtil = dbUtil;
	}
	
	@Override
	public boolean hasNext() throws LogException {
		return dbUtil.hasNext(rset);
	}

	@Override
	public LogRecord next() throws LogException {
		return dbUtil.getNextLog(rset);
	}

	@Override
	public void endScan() throws LogException {
		dbUtil.endScan(conn, pstmt, rset);
	}
}
