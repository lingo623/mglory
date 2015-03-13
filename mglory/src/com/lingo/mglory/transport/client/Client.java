package com.lingo.mglory.transport.client;

import java.util.Properties;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.transport.handler.ClientHandler;
import com.lingo.mglory.util.PropertiesUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
public class Client 
{
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	private int CONNECT_TIMEOUT_MILLIS=Integer.parseInt(config.getProperty("clientTimeOut"));
	private static Client client;
	private  Client()
	{
		
	}
	public synchronized static Client getInstance()
	{
		if (client==null)
		{
			client=new Client();
		}
		return client;
	}
    public MgloryOut call(String ip,int port,MgloryIn mgloryIn) 
    {
        EventLoopGroup group = new NioEventLoopGroup();
        MgloryOut mgloryOut=new MgloryOut();
        try
        {
            Bootstrap b = new Bootstrap();
            b.group(group);
            final ClientHandler chl=new ClientHandler();
            b.channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("encode", new ObjectEncoder());  
                    pipeline.addLast("decode", new ObjectDecoder(ClassResolvers.weakCachingConcurrentResolver(null)));
                    pipeline.addLast("handler", chl);
                }
            });
            ChannelFuture f = b.connect(ip, port).sync();
            f.channel().writeAndFlush(mgloryIn);
            f.channel().closeFuture().sync();
            mgloryOut=chl.getMessage();
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        } 
        finally
        {
            group.shutdownGracefully();
        }
        return mgloryOut;
    }
}
