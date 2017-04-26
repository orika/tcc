package com.mogujie.tcc.coordinator.id;


public interface IdForCoordinator {
	/**
	 * Description: get server id
	 * @return serverid
	 */
	int get();
	
	/**
	 * Description: determine whether a uuid is alloc by itself
	 * @param uuid
	 * @return true is the uuid is alloc by itself
	 */
	boolean isUuidOwn(long uuid);
}
