package com.mogujie.tcc.coordinator.recover;


public interface RecoverManager {
	
	/**
	 * Description: read log and recover the active transaction table
	 */
	void init(); 
	
	/**
	 * Description: get max uuid after recovery
	 * @return read max uuid
	 */
	long getLastMaxUUID();
}
