package com.test;

import com.lingo.mglory.resource.ResourceHelper;

public class Test5 {
	public static void main(String[] args) throws Exception
	{
		for(int i=0;i<10;i++)
		{
			long t=System.currentTimeMillis();
			String resource=ResourceHelper.getInstance().getIdleResource("114.215.178.115:1099","114.215.178.115:1099");
			System.out.println("resource==>"+resource);
			System.out.println("耗时==>"+(System.currentTimeMillis()-t));
			//Thread.sleep(3000);
		}

	}
}
