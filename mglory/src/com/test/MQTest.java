package com.test;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.mq.MgloryMQ;
import com.lingo.mglory.param.MgloryOut;

public class MQTest {
	private static Log log = LogFactory.getLog(MQTest.class);
	public static void main(String[] args) throws Exception
	{
		String topic="mqtest";
		String consumerId="sunxuhang";
		String consumerAddr="114.215.178.115:1099";
		String consumerMethod="com.test.MQTest.consume";
		subscribe(topic,consumerId,consumerAddr,consumerMethod);
		for (int i=0;i<20;i++)
		{
			send(topic,"123","helloTest123","hello mgloryMq===>"+i);
			System.out.println("test====>"+i);
		}
		System.exit(0);
	}
	private static void subscribe(String topic,String consumerId,String consumerAddr,String consumerMethod) throws Exception
	{
		MgloryMQ mq=new MgloryMQ("114.215.178.115:1099","114.215.178.115:1099");
		mq.subscribe(topic, consumerId, consumerAddr, consumerMethod);
	}
	public static void send(String topic,String produceId,String msgId,String msg) throws Exception
	{
		MgloryMQ mq=new MgloryMQ("114.215.178.115:1099","114.215.178.115:1099");
		mq.publish(topic, produceId, msgId, msg);
	}
	public MgloryOut consume(String taskId,Map<String,Object> inMap)
	{
		log.info("msg===>"+inMap.get("msg"));
		MgloryOut out=new MgloryOut();
		return out;
	}
	
}
