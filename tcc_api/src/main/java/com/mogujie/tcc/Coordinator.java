package com.mogujie.tcc;

import java.util.List;

import com.mogujie.tcc.common.HeuristicsInfo;
import com.mogujie.tcc.error.CoordinatorException;

public interface Coordinator {	
	long begin(int sequenceId, List<Procedure> procedures) throws CoordinatorException;

//	short confirm(int sequenceId, long uuid, List<Procedure> procedures) throws CoordinatorException;
	
	short confirm(int sequenceId, long uuid, long timeout, List<Procedure> procedures) throws CoordinatorException;
	
//	short cancel(int sequenceId, long uuid, List<Procedure> procedures) throws CoordinatorException;
	
	short cancel(int sequenceId, long uuid, long timeout, List<Procedure> procedures) throws CoordinatorException;
	
	List<HeuristicsInfo> getHeuristicExceptionList(long startTime, long endTime) throws CoordinatorException;
	
	void removeHeuristicExceptions(List<Long> txIdList) throws CoordinatorException;
	
	/*
	 * for test
	 */
	int getTxTableSize();
}