package com.mogujie.tcc.coordinator;

import com.mogujie.tcc.coordinator.config.CoordinatorConfig;
import com.mogujie.tcc.Participant;
import com.mogujie.tcc.error.ParticipantException;
import com.mogujie.tesla.client.api.TeslaServiceConsumerFactory;
import com.mogujie.tesla.core.ReferConfig;
import com.mogujie.tesla.generic.GenericException;
import com.mogujie.tesla.generic.GenericService;

public class ParticipantProxy implements Participant {

	private static final String PCLZ = "com.mogujie.tcc.error.ParticipantException";
	private static final String CONFIRM = "confirm";
	private static final String CANCEL = "cancel";
	private static final String EXPIRED = "expired";
	
	private static final String[] LONG_PARAM = new String[] {"java.lang.Long"};
	
	private GenericService participant = null;
	private long lastFailedTs = 0;
	private long retryInterval;
	
	public ParticipantProxy(long retryInterval) {
		this.retryInterval = retryInterval;
	}
	
	private ParticipantException getParticipantExp(GenericException e) {
		String clz = e.getExceptionClass();
		if (clz.equals(PCLZ)) {
			String msg = e.getExceptionMessage();
			if (msg.startsWith("#")) {
				int index = msg.indexOf(':');
				short code = Short.valueOf(msg.substring(1, index));
				return new ParticipantException(msg.substring(index), code);
			}
			return new ParticipantException(msg);
		}
		return null;
	}
	
	@Override
	public void cancel(Long uuid) throws ParticipantException {
		try {
			participant.$invoke(CANCEL, LONG_PARAM, new Object[] {uuid});
		} catch (GenericException e) {
			ParticipantException exp = getParticipantExp(e);
			if (exp != null)
				throw exp;
			throw e;
		}
	}

	@Override
	public void confirm(Long uuid) throws ParticipantException {
		try {
			participant.$invoke(CONFIRM, LONG_PARAM, new Object[] {uuid});
		} catch (GenericException e) {
			ParticipantException exp = getParticipantExp(e);
			if (exp != null)
				throw exp;
			throw e;
		}
	}

	@Override
	public void expired(Long uuid) throws ParticipantException {
		try {
			participant.$invoke(EXPIRED, LONG_PARAM, new Object[] {uuid});
		} catch (GenericException e) {
			ParticipantException exp = getParticipantExp(e);
			if (exp != null)
				throw exp;
			throw e;
		}
	}
	
	public void invoke(String methodName, Object[] params) throws ParticipantException {
		String[] paramTypes = new String[params.length];
		for (int i = 0, j = params.length; i < j; i++)
			paramTypes[i] = params[i].getClass().getName();
		try {
			participant.$invoke(methodName, paramTypes, params);
		} catch (GenericException e) {
			ParticipantException exp = getParticipantExp(e);
			if (exp != null)
				throw exp;
			throw e;
		}
	}
	
	public synchronized boolean init(String service, String version, CoordinatorConfig config) {
		if (participant != null) {
			return true;
		}
		if (lastFailedTs != 0 && !shouldRetry()) {
			return false;
		}

		ReferConfig referConfig = new ReferConfig(service);
		referConfig.setGeneric(true);
		if (version != null)
			referConfig.setVersion(version);

		String group = config.getAppGroup();
		if (group != null)
			referConfig.setGroup(group);

		int timeout = config.getRpcTimeout();
		if (timeout > 0)
			referConfig.setTimeout(timeout);

		try {
			participant = (GenericService) TeslaServiceConsumerFactory.getTeslaServiceConsumer(referConfig);
			lastFailedTs = 0;
		} catch (Exception e) {
			lastFailedTs = System.currentTimeMillis();
			return false;
		}
		return true;
	}
	
	public boolean isInitialized() {
		return participant != null;
	}
	
	private boolean shouldRetry() {
		return lastFailedTs + retryInterval < System.currentTimeMillis();
	}
}
