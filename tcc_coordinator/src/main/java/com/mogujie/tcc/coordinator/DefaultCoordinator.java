package com.mogujie.tcc.coordinator;

import java.util.List;

import com.mogujie.tcc.coordinator.task.ServiceTask;
import com.mogujie.tcc.coordinator.transaction.TxManager;
import org.apache.log4j.Logger;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import com.mogujie.tcc.coordinator.config.CoordinatorConfig;
import com.mogujie.tcc.coordinator.transaction.Transaction;
import com.mogujie.tcc.Coordinator;
import com.mogujie.tcc.Procedure;
import com.mogujie.tcc.TccCode;
import com.mogujie.tcc.common.Action;
import com.mogujie.tcc.common.HeuristicsInfo;
import com.mogujie.tcc.common.IllegalActionException;
import com.mogujie.tcc.common.LogException;
import com.mogujie.tcc.error.CoordinatorException;
import com.mogujie.tcc.error.HeuristicsException;

public class DefaultCoordinator implements Coordinator {
	
	private static final Logger logger = Logger.getLogger("Coordinator");
	private TxManager txManager = null;
	private CoordinatorConfig config = null;
	private ServiceConfig<Coordinator> service = null;
	
	public DefaultCoordinator(TxManager txManager, CoordinatorConfig config) {
		this.txManager = txManager;
		this.config = config;
		
	}
	
	public void start() {
		String registerAddr = config.getZkAddress();
		String appName = config.getAppName();
		if (registerAddr == null || appName == null)
			throw new IllegalArgumentException("registerAddr and appName can not be null,check config");
		String version = config.getVersion();
		int port = config.getPort();
		String group = config.getGroup();
		
		ApplicationConfig application = new ApplicationConfig();
		application.setName(appName);
		 
		RegistryConfig registry = new RegistryConfig();
		registry.setAddress(registerAddr);
		
		ProtocolConfig protocol = new ProtocolConfig();
		protocol.setName("dubbo");
		protocol.setPort(port);
		protocol.setThreadpool("cached");
		 
		service = new ServiceConfig<Coordinator>();
		service.setApplication(application);
		service.setRegistry(registry);
		service.setProtocol(protocol);
		service.setLoadbalance("consistenthash");
		service.setInterface(Coordinator.class);
		service.setRef(this);
		service.setVersion(version);
		if (group != null)
			service.setGroup(group);
		 
		service.export();
	}
	
	public void stop() {
		service.unexport();
		service = null;
	}
	
	private void preCheck(List<Procedure> procedures, Action action) {
		for (int i = 0, j = procedures.size(); i < j; i++) {
			Procedure proc = procedures.get(i);
			if (proc.getMethod() == null) {
				ServiceTask.setSignature(proc, action);
			}
			proc.setIndex(i);
		}
	}

	public long begin(int sequenceId, List<Procedure> expireGroups) throws CoordinatorException {
		Transaction tx = null;
		tx = txManager.createTx(expireGroups);
		return tx.getUUID();
	}
	
	public short confirm(int sequenceId, long uuid, List<Procedure> procedures) 
			throws CoordinatorException {
		logger.debug("process:" + procedures);
		preCheck(procedures, Action.CONFIRM);
		try {
			txManager.perform(uuid, Action.CONFIRM, procedures);
			return 0;
		} catch (HeuristicsException e) {
			return e.getCode();
		} catch (IllegalActionException e) {
			logger.error("transaction " + uuid + " confirm error.", e);
			throw new CoordinatorException(e);
		} catch (LogException e) {
			logger.error("transaction " + uuid + " confirm error.", e);
			throw new CoordinatorException(e);
		}
	} 
	
	public short confirm(int sequenceId, final long uuid, long timeout, final List<Procedure> procedures) 
			throws CoordinatorException {
		logger.debug("process:" + procedures);
		preCheck(procedures, Action.CONFIRM);
		try {
			txManager.perform(uuid, Action.CONFIRM, procedures, timeout);
			return TccCode.OK;
		} catch (HeuristicsException e) {
			return e.getCode();
		}  catch (IllegalActionException e) {
			logger.error("transaction " + uuid + " confirm error.", e);
			throw new CoordinatorException(e);
		} catch (LogException e) {
			logger.error("transaction " + uuid + " confirm error.", e);
			throw new CoordinatorException(e);
		}
	}

	@Override
	public short cancel(int sequenceId, long uuid, List<Procedure> procedures) 
			throws CoordinatorException {
		logger.debug("process:" + procedures);
		preCheck(procedures, Action.CANCEL);
		try {
			txManager.perform(uuid, Action.CANCEL, procedures);
			return TccCode.OK;
		} catch (HeuristicsException e) {
			return e.getCode();
		} catch (IllegalActionException e) {
			logger.error("transaction " + uuid + " cancel error.", e);
			throw new CoordinatorException(e);
		} catch (LogException e) {
			logger.error("transaction " + uuid + " cancel error.", e);
			throw new CoordinatorException(e);
		}
	}

	@Override
	public short cancel(int sequenceId, long uuid, long timeout, List<Procedure> procedures) 
			throws CoordinatorException {
		logger.debug("process:" + procedures);
		preCheck(procedures, Action.CANCEL);
		try {
			txManager.perform(uuid, Action.CANCEL, procedures, timeout);
			return TccCode.OK;
		} catch (HeuristicsException e) {
			return e.getCode();
		} catch (IllegalActionException e) {
			logger.error("transaction " + uuid + " cancel error.", e);
			throw new CoordinatorException(e);
		} catch (LogException e) {
			logger.error("transaction " + uuid + " cancel error.", e);
			throw new CoordinatorException(e);
		}
	}

	@Override
	public int getTxTableSize() {
		return txManager.getTxTable().getSize();
	}

	@Override
	public List<HeuristicsInfo> getHeuristicExceptionList(long startTime,
			long endTime) throws CoordinatorException {
		return txManager.getHeuristcisExceptionList(startTime, endTime);
	}

	@Override
	public void removeHeuristicExceptions(List<Long> txIdList)
			throws CoordinatorException {
		txManager.removeHeuristicsExceptions(txIdList);
	}
}