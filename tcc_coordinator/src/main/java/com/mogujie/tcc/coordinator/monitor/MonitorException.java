package com.mogujie.tcc.coordinator.monitor;

import com.mogujie.tcc.error.CoordinatorException;

public class MonitorException extends CoordinatorException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8943650879405912407L;

	public MonitorException(String message, Throwable t) {
		super(message, t);
	}
}
