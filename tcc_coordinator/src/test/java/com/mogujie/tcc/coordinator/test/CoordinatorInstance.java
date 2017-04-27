package com.mogujie.tcc.coordinator.test;

import java.io.IOException;

import com.mogujie.tcc.coordinator.ServiceContext;
import com.mogujie.tcc.coordinator.TccContainer;
import com.mogujie.tcc.Coordinator;

public class CoordinatorInstance {
	
	private static Coordinator coordinator = null;
	
	public static Coordinator get() {
		return coordinator;
	}
	
	public static void touch() {
		if (coordinator == null)
			init();
	}

	public static void init() {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				TccContainer.main(new String[]{"classpath*:/test/*.xml"});
			}
		});
		thread.start();
		while (coordinator == null) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (ServiceContext.getApplicationContext() == null)
				continue;
			coordinator = ServiceContext.getCoordinator();
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
