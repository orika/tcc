package com.mogujie.tcc.coordinator.monitor;

import com.mogujie.tcc.coordinator.config.CoordinatorConfig;
import com.mogujie.tcc.coordinator.id.IdForCoordinator;
import com.mogujie.tcc.coordinator.transaction.TxManager;
import com.mogujie.tcc.coordinator.util.DbUtil;
import com.mogujie.tcc.coordinator.util.MonitorUtil;

public class DBTccMonitor extends TccMonitor {
	
	private DbUtil dbUtil = null;
	private MonitorUtil monUtil = null;
	
	public DBTccMonitor(CoordinatorConfig config, TxManager manager, IdForCoordinator idForCoordinator) {
		super(config.getMonitorInterval(), manager.getGlobalMetric(), idForCoordinator);
	}

	public void setDbUtil(DbUtil dbUtil) {
		this.dbUtil = dbUtil;
	}
	
	public void setMonitorUtil(MonitorUtil monUtil) {
		this.monUtil = monUtil;
	}

	@Override
	public void write(MonitorRecord rec) throws MonitorException {
		// record in monitor platform
		monUtil.writeMonitorRec(rec);
		
		// record in system database
		dbUtil.writeMonitorRec(rec);
	}
}
