package com.mogujie.tcc.coordinator.log.db;

import java.util.List;

import com.mogujie.tcc.coordinator.log.Checkpoint;
import com.mogujie.tcc.coordinator.log.LogManager;
import com.mogujie.tcc.coordinator.log.LogScanner;
import com.mogujie.tcc.coordinator.log.LogType;
import com.mogujie.tcc.coordinator.util.DbUtil;
import org.apache.log4j.Logger;

import com.mogujie.tcc.coordinator.transaction.Transaction;
import com.mogujie.tcc.common.Action;
import com.mogujie.tcc.common.HeuristicsInfo;
import com.mogujie.tcc.common.IllegalActionException;
import com.mogujie.tcc.common.LogException;
import com.mogujie.tcc.error.HeuristicsException;

public class LogManagerImp implements LogManager {

	private DbUtil dbUtil = null;
	private static final Logger logger = Logger.getLogger(LogManagerImp.class);
	
	public LogManagerImp() {
	}
	
	public void setDbUtil(DbUtil dbUtil) {
		this.dbUtil = dbUtil;
	}

	@Override
	public void logBegin(Transaction tx, Action action) throws LogException {	
		// Get log Type
		LogType logType = null;
		switch(action) {
		case CONFIRM:
			logType = LogType.TRX_START_CONFIRM;
			break;
		case CANCEL:
			logType = LogType.TRX_START_CANCEL;
			break;
		case EXPIRE:
			logType = LogType.TRX_START_EXPIRE;
			break;
		default:
			throw new LogException("Action Type Error in logBegin");
		}
		
		this.dbUtil.writeLog(tx, logType);
	}

	@Override
	public void logFinish(Transaction tx, Action action) throws LogException {
		// Get log Type
		LogType logType = null;
		switch(action) {
		case CONFIRM:
			logType = LogType.TRX_END_CONFIRM;
			break;
		case CANCEL:
			logType = LogType.TRX_END_CANCEL;
			break;
		case EXPIRE:
			logType = LogType.TRX_END_EXPIRE;
			break;
		default:
			throw new LogException("Action Type Error in logFinish");
		}
		this.dbUtil.writeLog(tx, logType);
	}

	@Override
	public void logRegister(Transaction tx) throws LogException {
		LogType logType = LogType.TRX_BEGIN;
		
		this.dbUtil.writeLog(tx, logType);
	}

	@Override
	public boolean checkExpire(long uuid) throws LogException {
		boolean res = this.dbUtil.checkExpire(uuid);
		return res;
	}

	@Override
	public void logHeuristics(Transaction tx, Action action,
			HeuristicsException e) throws LogException {
		try {
			this.dbUtil.writeHeuristicRec(tx, action, e, false);
		} catch (LogException e1) {
			logger.error("Write system heuristic record error", e1);
			this.dbUtil.writeHeuristicRec(tx, action, e, true);
		}
		LogType logType = LogType.TRX_HEURESTIC;
		
		try {
			this.dbUtil.writeLog(tx, logType);
		} catch (LogException e2) {
			logger.error("Write heuristic log error", e2);
		}
	}

	@Override
	public void setCheckpoint(Checkpoint checkpoint) throws LogException {
		this.dbUtil.setCheckpoint(checkpoint);
	}

	@Override
	public Checkpoint getCheckpoint() throws LogException {
		return this.dbUtil.getCheckpoint();
	}

	@Override
	public void checkRetryAction(long uuid, Action action) throws LogException, IllegalActionException{
		this.dbUtil.checkRetryAction(uuid, action);
	}

	@Override
	public boolean checkLocalLogMgrAlive() {
		boolean res = this.dbUtil.checkLocaLogMgrAlive();
		return res;
	}

	@Override
	public LogScanner beginScan(long startpoint) throws LogException {
		return dbUtil.beginScan(startpoint);
	}

	@Override
	public List<HeuristicsInfo> getHeuristicsExceptionList(long startTime, long endTime)
			throws LogException {
		return dbUtil.getHeuristicsExceptionList(startTime, endTime);
	}

	@Override
	public void removeHeuristicsInfos(List<Long> txIdList)
			throws LogException {
		dbUtil.removeHeuristicsExceptions(txIdList);
	}
}


