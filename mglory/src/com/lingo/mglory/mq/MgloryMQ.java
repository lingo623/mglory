package com.lingo.mglory.mq;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceHelper;
import com.lingo.mglory.store.MgloryMap;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.util.Utils;

public class MgloryMQ{
	private static Log log = LogFactory.getLog(MgloryMQ.class);
	private String masterGroup;//中心服务（资源管理器）主机地址 如：114.215.178.115:1099
	private String slaveGroup;//中心服务（资源管理器）备机地址
	public MgloryMQ(String masterGroup,String slaveGroup){
		this.masterGroup=masterGroup;
		this.slaveGroup=slaveGroup;
	}
	/**
	 * 订阅(发布订阅模式)
	 * @param topic
	 * @param consumerId
	 * @param consumerAddr
	 * @param msgModel
	 * @throws Exception
	 */
	public void subscribe(String topic,String consumerId,String consumerAddr,String consumerMethod) throws Exception
	{
		if (topic==null || topic.equals("") ||consumerId==null || consumerId.equals("") || consumerAddr==null || consumerAddr.equals("")
		 || consumerMethod==null || consumerMethod.equals(""))
		{
			throw new Exception("输入参数不合法");
		}
		Map<String,Object> valueMap=new HashMap<String,Object>();
		valueMap.put("topic",topic);
		valueMap.put("consumerId", consumerId);
		valueMap.put("consumerAddr",consumerAddr);
		valueMap.put("consumerMethod",consumerMethod);
		String value=Utils.transMapToStr(valueMap);
		MgloryMap mgloryMap=new MgloryMap(masterGroup,slaveGroup);
		try {
			mgloryMap.put(topic+"__"+consumerId,value);
		} catch (Exception e) {
			throw new Exception("订阅主题:"+topic+"失败:"+e);
		}
	}
	/**
	 * 取消订阅(发布订阅模式)
	 * @param topic
	 * @param subscriber
	 * @throws Exception
	 */
	public void unsubscribe(String topic)throws Exception
	{
		MgloryMap mgloryMap=new MgloryMap(masterGroup,slaveGroup);
		MgloryOut mgloryOut=mgloryMap.getAllByRootKey(topic);
		Map<String,Object> outMap=mgloryOut.getMap();
		if (!outMap.isEmpty())
		{
			for (String key:outMap.keySet())
			{
				try {
					mgloryMap.remove(key);
				} catch (Exception e) {
					throw new Exception("取消订阅主题:"+topic+"失败:"+e);
				}
			}
		}
	}
	/**
	 * 发布消息
	 * @param topic
	 * @param produceId
	 * @param msgId
	 * @param msg
	 * @throws Exception
	 */
	public int publish(String topic,String produceId,String msgId,String msg) throws Exception
	{
		log.info("msg====>"+msg);
		int result=-1;
		if (topic==null || topic.equals("") ||produceId==null || produceId.equals("") || msgId==null || msgId.equals(""))
		{
			throw new Exception("输入参数不合法");
		}
		String resource=ResourceHelper.getInstance().getIdleResource(masterGroup, slaveGroup);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("topic", topic);
		inMap.put("produceId",produceId);
		inMap.put("msgId",msgId);
		inMap.put("msg",msg);
		MgloryIn mgloryIn=new MgloryIn();
		mgloryIn.setInvokeMethod("com.lingo.mglory.mq.MQReceiver.receive");
		mgloryIn.setMap(inMap);
		MgloryOut out=Client.getInstance().call(ip, port, mgloryIn);
		if (out.getStatus().equals(MgloryOut.NORMAL))
		{
			result=0;
		}
		return result;
	}
}