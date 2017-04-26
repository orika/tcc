package com.mogujie.tcc.coordinator.id;

public interface UUIDGenerator {
	void init(long lastUUID);
	long next();
}
