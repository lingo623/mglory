package com.test;

import java.util.ArrayList;
import java.util.List;

import com.lingo.mglory.route.MgloryHash;
import com.lingo.mglory.route.Node;

public class Test4 {
	public static void main(String[] args)
	{
		System.out.println("name===>"+Test4.class.getName());
		long t=System.currentTimeMillis();
		List<Node<String>> nodes = new ArrayList<Node<String>>();// 真实机器节点
		for (int i=0;i<5;i++)
		{
			Node<String> node=new Node<String>("127.0.0."+i);
			nodes.add(node);
		}
		MgloryHash<Node<String>> mgloryHash=new MgloryHash<Node<String>>(1000,nodes);
		for (int i=0;i<10000;i++)
		{
			String key="hello"+i;
			mgloryHash.get(key);
			System.out.println(key+"===>"+mgloryHash.get(key));
		}
		System.out.println("耗时====>"+(System.currentTimeMillis()-t));
	}
}
