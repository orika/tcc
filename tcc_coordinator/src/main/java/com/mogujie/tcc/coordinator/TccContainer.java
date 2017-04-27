package com.mogujie.tcc.coordinator;

import java.io.IOException;

import com.mogujie.tcc.coordinator.id.UUIDGenerator;
import com.mogujie.tcc.coordinator.recover.RecoverManager;
import com.mogujie.tcc.coordinator.monitor.TccMonitor;
import com.mogujie.tcc.coordinator.transaction.TxManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource({"classpath*:/spring/*.xml","classpath*:META-INF/tesla/core/*.xml"})
public class TccContainer {
	
	private static final Logger logger = Logger.getLogger("TccContainer");
	
	private TccMonitor monitor;
	private RecoverManager recoverManager = null;
	private TxManager txManager = null;
	private UUIDGenerator uuidGenerator = null;
	private DefaultCoordinator coordinator = null;
	
	public TccContainer(TccMonitor monitor, RecoverManager recoverManager, 
			TxManager txManager, UUIDGenerator uuidGenerator, 
			DefaultCoordinator coordinator) {
		this.monitor = monitor;
		this.recoverManager = recoverManager;
		this.txManager = txManager;
		this.uuidGenerator = uuidGenerator;
		this.coordinator = coordinator;
	}
	
	public void start() {
		txManager.enableRetry();
		recoverManager.init();
		uuidGenerator.init(recoverManager.getLastMaxUUID());
//		coordinator.start();
		monitor.start();
		txManager.beginExpire();
	}
	
	public void stop() {
//		coordinator.stop();
	}

	public static void main(String[] args) {
		SpringApplication.run(TccContainer.class, args);
	}
	
	public TxManager getTxManager() {
		return txManager;
	}
}
