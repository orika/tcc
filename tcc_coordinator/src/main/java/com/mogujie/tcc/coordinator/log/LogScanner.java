package com.mogujie.tcc.coordinator.log;

import com.mogujie.tcc.common.LogException;


public interface LogScanner {
	/**
	 * check if has next log
	 * @return true is has next log
	 * @throws LogException
	 */
	boolean hasNext() throws LogException;
	
	/**
	 * fetch next log record
	 * @return log record
	 * @throws LogException
	 */
	LogRecord next() throws LogException;
	
	/**
	 * end scan and destory the scanner connection
	 * @throws LogException
	 */
	void endScan() throws LogException;
}
