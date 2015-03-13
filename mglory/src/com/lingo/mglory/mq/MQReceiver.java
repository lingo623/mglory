package com.lingo.mglory.mq;

import java.util.Map;

import com.lingo.mglory.param.MgloryOut;

public class MQReceiver{
	public MgloryOut receive(String taskId,Map<String,Object> inMap)throws Exception
	{
		MgloryOut out =new MgloryOut();
		MQStore.getInstance().msgQueue.add(inMap);
		return out;
	}
}
