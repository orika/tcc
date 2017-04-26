package com.mogujie.tcc.coordinator.task;

import com.mogujie.tcc.coordinator.transaction.Transaction;

public interface TxRetryWatcher {
	
	void processError(Transaction tx);
	
	void processSuccess(Transaction tx);
}
