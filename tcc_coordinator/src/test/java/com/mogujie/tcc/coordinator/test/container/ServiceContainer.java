package com.mogujie.tcc.coordinator.test.container;

import com.mogujie.tcc.Participant;

public interface ServiceContainer extends Participant {
	
	void tryDo();
	
	Service getService();

	void setService(Service service);
	
	Object getResult();
}
