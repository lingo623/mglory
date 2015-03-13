package com.test;

import java.rmi.RemoteException;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceUsage;

public class ResourceUsageTest{
	public ResourceUsageTest() throws RemoteException {
		super();
	}
	/**
	 * 
	 */
	public MgloryOut task(MgloryIn in) throws RemoteException
	{
		MgloryOut out=new MgloryOut();
		System.out.println("cpu:"+ResourceUsage.getCpuUsage());
		System.out.println("io:"+ResourceUsage.getIoUsage());
		System.out.println("memory:"+ResourceUsage.getFreeMemory());
		System.out.println("net:"+ResourceUsage.getNetUsage());
		
		out.setStatus(MgloryOut.NORMAL);
		return out;
	}
	public static void main(String[] args)throws Exception
	{
		System.out.println("cpu:"+ResourceUsage.getCpuUsage());
		System.out.println("io:"+ResourceUsage.getIoUsage());
		System.out.println("memory:"+ResourceUsage.getFreeMemory());
		System.out.println("net:"+ResourceUsage.getNetUsage());
	}
}
