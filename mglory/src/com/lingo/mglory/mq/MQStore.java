package com.lingo.mglory.mq;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.store.MgloryMap;
import com.lingo.mglory.util.PropertiesUtil;


public class MQStore {
	private static Log log = LogFactory.getLog(MQStore.class);
	private static MQStore mqStore;
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	public ConcurrentLinkedQueue<Map<String,Object>> msgQueue=new ConcurrentLinkedQueue<Map<String,Object>>();
	//存储已经发送的消费者（消息整体没发送完）
	private ConcurrentMap<String,ConcurrentLinkedQueue<String>> haveConsumedMap =new ConcurrentHashMap<String,ConcurrentLinkedQueue<String>>();
	private Map<String,Map<String,Object>> topicMap =new HashMap<String,Map<String,Object>>();
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
		getTopic();
		ScheduledExecutorService dealExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
		MsgExecutorTask msgExecutorTask=new MsgExecutorTask();
		msgExecutorTask.setMsgQueue(msgQueue);
		msgExecutorTask.setHaveConsumedMap(haveConsumedMap);
		msgExecutorTask.setTopic(topicMap);
		msgExecutorTask.setScheduler(dealExecutorService);
		dealExecutorService.scheduleWithFixedDelay(msgExecutorTask, 1,1, TimeUnit.SECONDS);
	}
	private void getTopic()
	{
		String masterIp=config.getProperty("masterIp");
		String masterPort=config.getProperty("masterPort");
		String slaveIp=config.getProperty("slaveIp");
		String slavePort=config.getProperty("slavePort");
		final String masterGroup=masterIp+":"+masterPort;
		final String slaveGroup=slaveIp+":"+slavePort;
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
				new Callable<Map<String,Object>>() {
					@Override
					public Map<String,Object> call() throws Exception{
						while(true)
						{
							MgloryMap mgloryMap=new MgloryMap(masterGroup,slaveGroup);
							try {
								MgloryOut mgloryOut=mgloryMap.getAllByRootKey("topic");
								Map<String,Object> outMap=mgloryOut.getMap();
								if (!outMap.isEmpty())
								{
									for (String key:outMap.keySet())
									{
										String topic=outMap.get(key).toString();
										try
										{
											MgloryOut topicOut=mgloryMap.getAllByRootKey(topic);
											Map<String,Object> topicOutMap=topicOut.getMap();
											topicMap.put(topic,topicOutMap);
										}
										catch(Exception e)
										{
											log.warn("忽略异常:"+e);
										}
										
									}
								}
								else{
									topicMap.clear();
								}
							} catch (Exception e) {
								log.warn("忽略异常:"+e);
							}
							Thread.sleep(3000);
						}
					}
		});
		executor.submit(futureTask);
		executor.shutdown();
	}
}
