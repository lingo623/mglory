package com.lingo.mglory.transport.handler;


import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;

import io.netty.channel.ChannelHandlerContext;

import io.netty.channel.ChannelInboundHandlerAdapter;


public class ServerHandler extends ChannelInboundHandlerAdapter 
{
	private static Log log = LogFactory.getLog(ServerHandler.class);
	private boolean taskCompleteFlag;
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception 
    {
    	MgloryIn mgloryIn=(MgloryIn)msg;
        //log.debug("SERVER接收方法调用==>"+mgloryIn.getInvokeMethod());
        //log.debug("SERVER接收参数==>"+mgloryIn.getMap());
        String taskId=mgloryIn.getTaskId();
        String invokeMethod=mgloryIn.getInvokeMethod();
        Map<String,Object> inMap=mgloryIn.getMap();
        long timeout=mgloryIn.getTimeout();
        MgloryOut mgloryOut=invoke(invokeMethod,taskId,inMap,timeout);
        ctx.channel().writeAndFlush(mgloryOut);
        ctx.close();
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception 
    {
    	//log.info(">>>>>>>>");
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception 
    {
    	log.warn("server exception is general:"+cause);
    }
    public MgloryOut invoke(final String invokeMethod,final String taskId,final Map<String,Object> inMap,final long timeout)
    {
    	final long t=System.currentTimeMillis();
    	MgloryOut out=new MgloryOut();
		ExecutorService taskExecutor=Executors.newFixedThreadPool(1);
		final FutureTask<MgloryOut> futureTask = new FutureTask<MgloryOut>(
				new Callable<MgloryOut>() {
					@Override
					public MgloryOut call() throws Exception {
						return invoke(invokeMethod,taskId,inMap);
					}
		});
		taskExecutor.submit(futureTask);
		ExecutorService interruptedExecutor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> interruptedTask = new FutureTask<Map<String,Object>>(
				new Callable<Map<String,Object>>() {
					@Override
					public Map<String,Object> call() throws Exception {
						while(!taskCompleteFlag)
						{
							if (timeout!=0 && System.currentTimeMillis()-t>timeout)
							{
								//终止任务执行
								if(!futureTask.isCancelled())
								{
									futureTask.cancel(true);
								}
								taskCompleteFlag=true;
								break;
							}
							else
							{
								if (timeout==0)
								{
									break;
								}
								else
								{
									Thread.sleep(1000);
								}
							}
						}
						return null;
					}
		});
		interruptedExecutor.submit(interruptedTask);
		interruptedExecutor.shutdown();
		try
		{
			out=futureTask.get();
		}
		catch(Exception e)
		{
			//throw new RuntimeException(e);
			out.setStatus(MgloryOut.INTERRUPT);
			out.setErrorMsg(e.getMessage());
		}
		finally
		{
			taskCompleteFlag=true;
			taskExecutor.shutdown();
		}
		return out;
    }
    public MgloryOut invoke(String invokeMethod,final String taskId,Map<String,Object> inMap)
    {
    	MgloryOut mgloryOut=new MgloryOut();
    	if (invokeMethod.equals(""))
    	{
    		mgloryOut.setStatus(MgloryOut.EXCEPTION);
    		mgloryOut.setErrorMsg("类方法名不能为空");
    	}
    	else
    	{
        	String className="";
    		String methodName="";
    		int lastPointIndex=invokeMethod.lastIndexOf(".");
    		className=invokeMethod.substring(0,lastPointIndex);
    		methodName=invokeMethod.substring(lastPointIndex+1,invokeMethod.length());
    		try{
    		     Class<?> cls = Class.forName(className);
    		     Object instance = cls.newInstance();
    		     Method method = cls.getMethod(methodName,String.class, Map.class);
    		     mgloryOut=(MgloryOut)method.invoke(instance,taskId,inMap);
    		}
    		catch(Exception e)
    		{
    			mgloryOut.setStatus(MgloryOut.EXCEPTION);
        		mgloryOut.setErrorMsg("执行异常："+e);
    		}
    	}
    	return mgloryOut;
    }
}
