package com.mogujie.tcc.coordinator.test.container;

import com.mogujie.tcc.Participant;

public interface Service extends Participant {
	void tryDo();
	
	Object getResult();
}
