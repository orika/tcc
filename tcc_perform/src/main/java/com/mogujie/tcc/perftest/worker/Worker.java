package com.mogujie.tcc.perftest.worker;

import com.mogujie.tcc.TccManager;
import com.mogujie.tcc.perftest.face.IPayment;
import com.mogujie.tcc.perftest.face.ISale;
import com.mogujie.tcc.perftest.TestContainer;

public abstract class Worker implements Runnable {
	
	protected volatile boolean stop = false;
	protected TccManager tccManager = null;
	protected IPayment payment = null;
	protected ISale sale = null;
	private Thread thread = null;
	
	protected volatile long count = 0;
	private int index;
	
	public Worker(int index, TestContainer container) {
		this.tccManager = (TccManager) container.getBean("tccManager");
		this.payment = (IPayment) container.getBean("payment");
		this.sale = (ISale) container.getBean("sale");
		this.index = index;
		this.thread = new Thread(this);
	}
	
	public void collectMetric(int time, Metric metric) {
		if (count == 0) {
			metric.addResTime(index, 0);
			return;
		}
		long c = count;
		count = 0;
		metric.addCount(c);
		metric.addResTime(index, time / c);
	}
	
	public void stop() {
		this.stop = true;
		try {
			this.thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void start() {
		thread.start();
	}
}
