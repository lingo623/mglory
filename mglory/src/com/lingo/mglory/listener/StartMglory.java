package com.lingo.mglory.listener;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.lingo.mglory.resource.NodeManager;
import com.lingo.mglory.server.MgloryWorkerServer;
import com.lingo.mglory.server.MgloryGroupServer;

public class StartMglory {
	public static void main(String[] args) {
		System.out.println("mglory start......");
		if (args.length < 1) {
			System.err.println("Usage:\tcom.lingo.mglory.server.StartMglory <config-file> program exit now.");
			System.exit(0);
		}
		String startType=args[0].trim();
		String lockName="";
		//集团服务
		if (startType.equals("group"))
		{
			try
			{
				MgloryGroupServer.getInstance().startGroupServer();
				NodeManager.getInstance().startHeart(true);
			}
			catch (Exception e)
			{
				System.err.println("start mgloryGroupServer failed, program exit now.");
				System.err.println(e);
				System.exit(0);
			}
			if (MgloryGroupServer.getInstance().getMasterFlag())
			{
				lockName="localhost_"+MgloryGroupServer.getInstance().getPort()+"_group_master";
			}
			else
			{
				lockName="localhost__"+MgloryGroupServer.getInstance().getPort()+"_group_slave";
			}
		}
		else
		{
			if (args.length < 4) {
				System.err.println("Usage:\tcom.lingo.mglory.server.StartMglory <config-file> program exit now.");
				System.exit(0);
			}
			String ip=args[1].trim();
			String port=args[2].trim();
			String resourceType=args[3].trim();
			lockName="localhost__"+port+"_worker";
			try
			{
				MgloryWorkerServer.getInstance().startResource(ip, Integer.parseInt(port),resourceType);
				NodeManager.getInstance().startHeart(false);
			}
			catch (Exception e)
			{
				System.err.println("start mgloryWorkerServer failed, program exit now.");
				System.err.println(e);
				System.exit(0);
			}
		}
		// 根据当前输入参数的文件名生成lock锁文件
		File flock = new File("./lock", lockName + ".lock");
		if (flock.exists()) {
			System.out.println("Lock file " + flock.getName() + " \t exists already,program continue.");
			System.out.println("flock :" + flock.getAbsolutePath());
		} else {
			try {
				if (flock.createNewFile()) {
					System.out.println("Lock successfully created");
				} else {
					System.out.println("Create Lock file failed,but this program continue");
				}
			} catch (IOException e) {
				System.err.println("Create File Lock failed, program exit now.");
				System.err.println(e);
				System.exit(0);
			}
		}
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
		ExitSystemExecutor exitSystemExecutor=new ExitSystemExecutor(flock);
		executorService.scheduleAtFixedRate(exitSystemExecutor, 1, 1, TimeUnit.SECONDS);
		System.out.println("mglory start complete......");
	}
}
