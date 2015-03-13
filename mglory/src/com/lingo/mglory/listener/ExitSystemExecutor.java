package com.lingo.mglory.listener;

import java.io.File;

public class ExitSystemExecutor implements Runnable{
	private File flock;
	public ExitSystemExecutor(File flock)
	{
		this.flock=flock;
	}
	public void run()
	{
		if(!flock.exists())
		{
			System.exit(0);
		}
	}
}
