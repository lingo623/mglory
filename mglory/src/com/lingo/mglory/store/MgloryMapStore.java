package com.lingo.mglory.store;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceHelper;
import com.lingo.mglory.server.MgloryWorkerServer;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.util.Utils;

public class MgloryMapStore {
	private static Log log = LogFactory.getLog(MgloryMapStore.class);
	private static MgloryMapStore mgloryMapStore;
	private String masterGroup;
	public boolean synFlag=false;
	public ConcurrentMap<String,Object> mgloryMap=new ConcurrentHashMap<String,Object>();
	public ConcurrentLinkedQueue<Map<String,Map<String,Object>>> operQueue=new ConcurrentLinkedQueue<Map<String,Map<String,Object>>>();
	public final Map<String, Method> methodMap = new HashMap<String, Method>();
	private MgloryMapStore(){
		init();
		if (synFlag)
		{
			synToBakTask();
		}
		//首次需要进行加载
		loadMemory();
		persistenceTask();
	}
	public synchronized static MgloryMapStore getInstance()
	{
		if (mgloryMapStore==null)
		{
			mgloryMapStore=new MgloryMapStore();
		}
		return mgloryMapStore;
	}
	private void init()
	{
		try
		{
			masterGroup=MgloryWorkerServer.getInstance().getMasterGroup();
			MgloryIn mgloryIn=new MgloryIn();
			mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.getBakExecResource");
			String ip=masterGroup.split(":")[0];
			int port=Integer.parseInt(masterGroup.split(":")[1]);
			MgloryOut mgloryOut=Client.getInstance().call(ip, port, mgloryIn);
			Map<String,Object> bakExecResourceMap=mgloryOut.getMap();
			String resource=MgloryWorkerServer.getInstance().getResource();
			if (bakExecResourceMap.containsKey(resource))
			{
				synFlag=true;
			}
			if (methodMap.isEmpty())
			{
				Method[] methods = new MgloryMapManager().getClass().getMethods();
				for (int i=0; i<methods.length; i++) {
					methodMap.put(methods[i].getName(), methods[i]);
				}
			}
		}
		catch(Exception e)
		{
			log.error("e====>"+e);
		}
	}
	private void synToBakTask()
	{
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
					new Callable<Map<String,Object>>() {
						@Override
						public Map<String,Object> call() throws Exception {
							String resource=ResourceHelper.getInstance().getFrontResource(masterGroup,masterGroup);
							String ip=resource.split(":")[0];
							int port=Integer.parseInt(resource.split(":")[1]);
							MgloryIn in=new MgloryIn();
							in.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.getBakExecResource");
							while(true)
							{
								try
								{
									MgloryOut out=Client.getInstance().call(ip, port, in);
									Map<String,Object> bakExecResourceMap=out.getMap();
									if (bakExecResourceMap.containsKey(resource))
									{
										String bakResource=bakExecResourceMap.get(resource).toString();
										List<Map<String,Map<String,Object>>> list=new ArrayList<Map<String,Map<String,Object>>>();
										for (int i=0;i<operQueue.size();i++)
										{
											list.add(operQueue.poll());
										}
										Map<String,Object> map=new HashMap<String,Object>();
										map.put("list",list);
										MgloryIn mgloryIn=new MgloryIn();
										mgloryIn.setInvokeMethod("com.lingo.mglory.application.MgloryMapApp.syn");
										mgloryIn.setMap(map);
										String bakIp=bakResource.split(":")[0];
										int bakPort=Integer.parseInt(bakResource.split(":")[1]);
										out=Client.getInstance().call(bakIp, bakPort, mgloryIn);
									}
								}
								catch(Exception e)
								{
									log.error("同步异常忽略不计:"+e);
								}
								Thread.sleep(600000);
							}
						}
						
		 });
		 executor.submit(futureTask);
		 executor.shutdown();
	}
	private void persistenceTask()
	{
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
					new Callable<Map<String,Object>>() {
						@Override
						public Map<String,Object> call() throws Exception {
							while(true)
							{
								try
								{
									persistence();
								}
								catch(Exception e)
								{
									log.error("同步异常忽略不计:"+e);
								}
								Thread.sleep(60000);
							}
						}
						
		 });
		 executor.submit(futureTask);
		 executor.shutdown();
	}
	public void loadMemory()
	{
		String resource=MgloryWorkerServer.getInstance().getResource();
		String port=resource.split(":")[1];
		String fileName="./db/mglory"+port+".db";
		File file=new File(fileName);
		if(file.exists())
		{
			InputStreamReader read=null;
			try
			{
				read = new InputStreamReader(new FileInputStream(file));
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while((lineTxt = bufferedReader.readLine()) != null){
					String key=lineTxt.split("<=>")[0];
					String value=lineTxt.split("<=>").length==2?lineTxt.split("<=>")[1]:null;
					//ConcurrentMap<String,String> map=transStrToMap(value);
					//mgloryMap.put(key,map);
					mgloryMap.put(key,value);
		        }
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				if (read!=null)
				{
					try {
						read.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	//持久化
	public void persistence()
	{
		long t=System.currentTimeMillis();
		String resource=MgloryWorkerServer.getInstance().getResource();
		String port=resource.split(":")[1];
		String fileName="./db/mglory"+port+".db";
		String tempFileName="./db/mglory"+port+t+".db";
		File file=new File(fileName);
		if(file.exists())
		{
			file.renameTo(new File(tempFileName));
		}
		else
		{
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		BufferedWriter bufferedWriter = null;
		try {
			 bufferedWriter = new BufferedWriter(new FileWriter(file));
			 for (String key:mgloryMap.keySet())
			 {
				 Object obj=mgloryMap.get(key);
				 String value=Utils.transMapToStr(obj);
				 bufferedWriter.write(key+"<=>"+value);				 
				 bufferedWriter.newLine();
			 }
		 } catch (Exception ex) {
			 ex.printStackTrace();
		 } finally {
		 //Close the BufferedWriter
		 try {
			 if (bufferedWriter != null) {
				 bufferedWriter.flush();
				 bufferedWriter.close();
			 }
			 } catch (IOException ex) {
				 ex.printStackTrace();
			 }
		 }
		 File tempFile=new File(tempFileName);
		 if (tempFile.exists())
		 {
			 tempFile.delete();
		 }
		 //loadMemory();
	}
}
