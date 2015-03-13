package com.lingo.mglory.mq;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.lingo.mglory.util.PropertiesUtil;


public class MQStore {
	private static MQStore mqStore;
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	public ConcurrentLinkedQueue<Map<String,Object>> msgQueue=new ConcurrentLinkedQueue<Map<String,Object>>();
	//存储已经发送的消费者（消息整体没发送完）
	private ConcurrentMap<String,ConcurrentLinkedQueue<String>> haveConsumedMap =new ConcurrentHashMap<String,ConcurrentLinkedQueue<String>>();
	private MQStore()
	{
		init();
	}
	public synchronized static MQStore getInstance()
	{
		if (mqStore==null)
		{
			mqStore=new MQStore();
		}
		return mqStore;
	}
	private void init()
	{
		String masterIp=config.getProperty("masterIp");
		String masterPort=config.getProperty("masterPort");
		String slaveIp=config.getProperty("slaveIp");
		String slavePort=config.getProperty("slavePort");
		String masterGroup=masterIp+":"+masterPort;
		String slaveGroup=slaveIp+":"+slavePort;
		ScheduledExecutorService dealExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
		MsgExecutorTask msgExecutorTask=new MsgExecutorTask();
		msgExecutorTask.setMsgQueue(msgQueue);
		msgExecutorTask.setHaveConsumedMap(haveConsumedMap);
		msgExecutorTask.setCluster(masterGroup, slaveGroup);
		msgExecutorTask.setScheduler(dealExecutorService);
		dealExecutorService.scheduleWithFixedDelay(msgExecutorTask, 1,1, TimeUnit.SECONDS);
	}
}
