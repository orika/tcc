/**
 * @Description:			Used for log management
 * Copy Right:				Netease
 * Project:					TCC
 * JDK Version				jdk-1.6
 * @version					1.0
 * @author					huwei				
 */

package com.netease.backend.coordinator.log;

import java.util.List;

import com.netease.backend.coordinator.transaction.Transaction;
import com.netease.backend.tcc.common.Action;
import com.netease.backend.tcc.common.HeuristicsInfo;
import com.netease.backend.tcc.common.IllegalActionException;
import com.netease.backend.tcc.common.LogException;
import com.netease.backend.tcc.error.HeuristicsException;

public interface LogManager {
	
	/**
	 * Description: log for transaction confirm/cancel/expire beginning
	 * @param tx    
	 * @param action
	 * @throws LogException
	 */
	void logBegin(Transaction tx, Action action) throws LogException;
	
	/**
	 * Description: log for transaction finishing
	 * @param tx    
	 * @param action
	 * @throws LogException
	 */
	void logFinish(Transaction tx, Action action) throws LogException;
	
	/**
	 * Description: log for transaction register
	 * @param tx    
	 * @param action
	 * @throws LogException
	 */
	void logRegister(Transaction tx) throws LogException;
	
	/**
	 * Description: log for transaction heuristics
	 * @param tx
	 * @param action
	 * @param e		return detail of heuristic
	 * @throws LogException
	 */
	void logHeuristics(Transaction tx, Action action, HeuristicsException e) throws LogException;
	
	/**
	 * Description: check expire operation is valid
	 * @param uuid
	 * @return true if expire is valid
	 * @throws LogException
	 */
	boolean checkExpire(long uuid) throws LogException;
	
	/**
	 * Description: set checkpoint
	 * @param checkpoint
	 * @throws LogException
	 */
	void setCheckpoint(Checkpoint checkpoint) throws LogException;
	
	/**
	 * Description: get checkpoint 
	 * @return checkpoint
	 * @throws LogException
	 */
	Checkpoint getCheckpoint() throws LogException;
	
	/**
	 * Description: check the action is valid if the tx is not belong to current node
	 * @param uuid
	 * @return true if operation is valid
	 * @throws LogException
	 * @throws IllegalActionException 
	 */
	void checkRetryAction(long uuid, Action action) throws LogException, IllegalActionException;
	
	/**
	 * Description: check local log system(RDS) is still alive
	 * @return
	 */
	boolean checkLocalLogMgrAlive();

	/**
	 * Description: alloc a log scanner
	 * @param startpoint
	 * @return a new log scanner
	 * @throws LogException
	 */
	LogScanner beginScan(long startpoint) throws LogException;
	
	/**
	 * Description: get HeuristicsException info within given time bucket
	 * @param startTime
	 * @param endTime
	 * @return HeuristicsInfo list
	 * @throws LogException 
	 */
	public List<HeuristicsInfo> getHeuristicsExceptionList(long startTime, long endTime) throws LogException;

	/**
	 * Description: remove HeuristicsExceptions from systemDB
	 * @param txId list
	 * @throws LogException 
	 */
	public void removeHeuristicsInfos(List<Long> txIdList) throws LogException;
}
