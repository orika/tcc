package com.mogujie.tcc.common;

import com.mogujie.tcc.error.CoordinatorException;


public class LogException extends CoordinatorException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8019056627152712074L;
	
	public LogException(String message, Throwable t) {
		super(message, t);
	}
	
	public LogException(String message) {
		super(message);
	}
}
