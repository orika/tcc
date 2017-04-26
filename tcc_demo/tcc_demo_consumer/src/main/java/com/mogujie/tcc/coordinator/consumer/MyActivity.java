package com.mogujie.tcc.coordinator.consumer;

import com.mogujie.tcc.Participant;
import com.mogujie.tcc.TccActivity;
import com.mogujie.tcc.TccManager;

public class MyActivity extends TccActivity {

	public MyActivity(TccManager tccManager, Participant[] participants) {
		super(tccManager, participants);
	}

	@Override
	public int[] getConfirmSeq() {
		return new int[]{0,0,1};
	}

	@Override
	public int[] getCancelSeq() {
		return new int[]{0,0,-1};
	}
}
