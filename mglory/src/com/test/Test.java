package com.test;

import java.util.HashMap;
import java.util.Map;

public class Test {
	public static void main(String[] args)
	{
		Map<String,Object> map1=new HashMap<String,Object>();
		Map<String,Object> map=new HashMap<String,Object>();
		for (int i=0;i<2;i++)
		{
			map.put("hello"+i, "word"+i);
		}
		map1.put("test", map);
		map=new HashMap<String,Object>();
		System.out.println("map1==>"+map1);
	}
}
