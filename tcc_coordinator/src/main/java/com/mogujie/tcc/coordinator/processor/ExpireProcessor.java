package com.mogujie.tcc.coordinator.processor;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mogujie.tcc.coordinator.task.ServiceTask;
import com.mogujie.tcc.coordinator.task.TxRetryWatcher;
import com.mogujie.tcc.coordinator.transaction.Transaction;
import org.apache.log4j.Logger;

import com.mogujie.tcc.Procedure;
import com.mogujie.tcc.common.Action;
import com.mogujie.tcc.common.IllegalActionException;

public class ExpireProcessor {
	
	private static final Logger logger = Logger.getLogger("ExpireProcessor");
	private RetryProcessor processor = null;
	private RetryWatcher watcher = new RetryWatcher();
	private Set<Transaction> filterSet = new HashSet<Transaction>();
	
	public ExpireProcessor(RetryProcessor processor) {
		this.processor = processor;
	}
	
	public void process(Transaction tx) {
		if (filterSet.contains(tx))
			return;
		List<Procedure> procList = tx.getExpireList();
		if (procList == null)
			return;
		for (Procedure proc : procList) {
			if (proc.getMethod() == null)
				ServiceTask.setSignature(proc, Action.EXPIRE);
		}
		try {
			tx.expire();
		} catch (IllegalActionException e) {
			return;
		}
		logger.info("expire " + tx);
		filterSet.add(tx);
		processor.process(tx, 1, watcher);
	}
	
	private class RetryWatcher implements TxRetryWatcher {
		
		@Override
		public void processError(Transaction tx) {
			filterSet.remove(tx);
		}

		@Override
		public void processSuccess(Transaction tx) {
			filterSet.remove(tx);
		}
	}
	
	public Set<Transaction> getExpiringTxSet() {
		return Collections.unmodifiableSet(filterSet);
	}
}
