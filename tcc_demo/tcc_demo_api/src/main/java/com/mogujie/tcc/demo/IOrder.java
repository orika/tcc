package com.mogujie.tcc.demo;

import com.mogujie.tcc.Participant;


public interface IOrder extends Participant {

	void available(String person, int count);
}
