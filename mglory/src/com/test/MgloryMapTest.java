/**
 * 分布式存储测试类
 */
package com.test;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lingo.mglory.store.MgloryMap;

public class MgloryMapTest{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6192616050186217549L;
	public MgloryMapTest() throws RemoteException {
		super();
	}
	public static void main(String[] args)throws Exception
	{
		List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
		long t3=System.currentTimeMillis();
		for (int i=1;i<10;i++)
		{
			Map<String,Object> map=new HashMap<String,Object>();
			map.put("key", "9121219283392192"+i);
			Map<String,Object> map1=new HashMap<String,Object>();
			map1.put("SERIAL_NUMBER", "18602567623");
			map1.put("TRADE_ID", "9718971897189718");
			map1.put("USER_ID", "9718971897189717");
			map1.put("CUST_NAME", "孙绪杭");
			map1.put("NET_TYPE_CODE", "0017");
			map1.put("TRADE_TYPE_CODE", "0010");
			map1.put("CERT_ADDR", "江苏省南京市中山南路弓箭坊40号305");
			map1.put("BRAND_CODE", "ADSL");
			map.put("value",map1);
			list.add(map);
		}
		MgloryMap mgloryMap=new MgloryMap("114.215.178.115:1099","114.215.178.115:1099");
		mgloryMap.batchPut(list);
		long t4=System.currentTimeMillis();
		System.out.println("value==>"+mgloryMap.get( "9121219283392192"+5));
		System.out.println("耗时==>"+(System.currentTimeMillis()-t4));
		System.out.println("耗时==>"+(System.currentTimeMillis()-t3));
		System.exit(0);
	}
}
