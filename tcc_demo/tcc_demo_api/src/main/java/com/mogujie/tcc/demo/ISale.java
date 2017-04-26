package com.mogujie.tcc.demo;

import com.mogujie.tcc.Participant;


public interface ISale extends Participant {

	int getPrice();
	
	int getItemCount();
	
	void sell(Long uuid, int count);
	
	void refond(Long uuid, int count);
	
	void reserve(Long uuid, int count);
	
	boolean isConfirmed(Long uuid);
}
