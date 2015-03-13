package com.lingo.mglory.mq;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceHelper;
import com.lingo.mglory.store.MgloryMap;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.util.Utils;

public class MsgExecutorTask extends TimerTask{
	private static Log log = LogFactory.getLog(MsgExecutorTask.class);
	private String masterGroup;
	private String slaveGroup;
	private ConcurrentLinkedQueue<Map<String,Object>> msgQueue;
	private ConcurrentMap<String,ConcurrentLinkedQueue<String>> haveConsumedMap;
	protected ExecutorService scheduler;
	public void setScheduler(ExecutorService scheduler) {
		this.scheduler = scheduler;
	}
	
	public void setMsgQueue(ConcurrentLinkedQueue<Map<String,Object>> msgQueue) {
		this.msgQueue = msgQueue;
	}
	public void setCluster(String masterGroup,String slaveGroup)
	{
		this.masterGroup=masterGroup;
		this.slaveGroup=slaveGroup;
	}
	public void setHaveConsumedMap(ConcurrentMap<String,ConcurrentLinkedQueue<String>> haveConsumedMap)
	{
		this.haveConsumedMap=haveConsumedMap;
	}
	public void run() {
		Map<String,Object> msgMap=msgQueue.poll();
		if (msgMap!=null)
		{
			try {
				String topic=msgMap.get("topic").toString();
				MgloryMap mgloryMap=new MgloryMap(masterGroup,slaveGroup);
				MgloryOut mgloryOut=mgloryMap.getAllByRootKey(topic);
				Map<String,Object> outMap=mgloryOut.getMap();
				if (outMap.isEmpty())
				{
					msgQueue.add(msgMap);
				}
				else
				{
					String msgId=msgMap.get("msgId").toString();
					for (String key:outMap.keySet())
					{
						String value=outMap.get(key).toString();
						Map<String,String> valueMap=Utils.transStrToMap(value);
						String consumerAddr=valueMap.get("consumerAddr");
						if (haveConsumedMap.get(msgId)!=null && haveConsumedMap.get(msgId).contains(consumerAddr))
						{
							continue;
						}
						else
						{
							String consumerMethod=valueMap.get("consumerMethod");
							try
							{
								String resource=ResourceHelper.getInstance().getIdleResource(consumerAddr, consumerAddr);

								String ip=resource.split(":")[0];
								int port=Integer.parseInt(resource.split(":")[1]);
								MgloryIn in=new MgloryIn();
								in.setMap(msgMap);
								in.setInvokeMethod(consumerMethod);
								MgloryOut out=Client.getInstance().call(ip, port,in);
								if (out.getStatus().equals(MgloryOut.NORMAL))
								{
									if (haveConsumedMap.get(msgId)==null)
									{
										ConcurrentLinkedQueue<String> queue=new ConcurrentLinkedQueue<String>();
										queue.add(consumerAddr);
										haveConsumedMap.put(msgId,queue);
									}
									else
									{
										haveConsumedMap.get(msgId).add(consumerAddr);
									}
								}
							}
							catch(Exception e)
							{
								log.warn("忽略异常:"+e);
							}
						}
					}
					if (haveConsumedMap.get(msgId).size()==outMap.size())
					{
						haveConsumedMap.remove(msgId);
					}
					else
					{
						//加入队列继续处理
						msgQueue.add(msgMap);
					}
				}
			} catch (Exception e) {
				log.warn("发送消息失败："+e);
			}
		}
	}
}
