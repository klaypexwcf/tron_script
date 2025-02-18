package myDiscover;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import myConnection.MyChannelManager;
import myDiscover.message.MyKadPingMessage;
import myDiscover.table.NodeId;
import myDiscover.table.NodeIdBucket;
import myDiscover.table.NodeIdTable;
import org.tron.p2p.discover.protocol.kad.table.NodeEntry;
import org.tron.p2p.stats.TrafficStats;
import test.NodeTableTest;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Main {
    private static final Map<Integer, MyMessageHandler> messageHandlerMap = new HashMap<>();
    private static int count = 0;
    //private static final NioEventLoopGroup group = new NioEventLoopGroup();
    /** 功 能  ：向一个节点持续发送Kad_ping，在目标节点回复Kad_ping时回复Kad_pong
     *  目标节点修改：如果Kad_ping的源端口是18899，则忽视discover表中ip地址的限制
     */
    public static void main(String[] args) throws InterruptedException {
        //初始化本地id、本地ipPort、目标节点ipPort
        MyConfig.init();
        MyConfig.test();
        //初始化netty框架

        //初始化NodeTable
        NodeIdTable nodeIdTable1 =new NodeIdTable();
        Byte[] byteId = Tool.toByteArray(MyConfig.getRemoteId());
        nodeIdTable1.init(new NodeId(byteId));
        //初始化TCP监听
        MyChannelManager.ListeningInit(18889,272,nodeIdTable1);
        //启动UDP Kad_ping 发送程序
        for (NodeIdBucket nodeIdBucket: nodeIdTable1.getNodeIdTable()){
            for (NodeId nodeId:nodeIdBucket.getNodeBucket()){
                Random random = new Random();
                Thread.sleep(random.nextInt(10));
                MyEventHandler myEventHandler=new MyEventHandler(null);
                NioEventLoopGroup group = new NioEventLoopGroup();
                Bootstrap bootstrap = getBootstrap(group, myEventHandler,nodeId,18889+count);
                Channel channel =bootstrap.bind( new InetSocketAddress("10.2.8.6",18889+count)).sync().channel();
                MyMessageHandler myMessageHandler = (MyMessageHandler) channel.pipeline().last();
                messageHandlerMap.put(18889+count,myMessageHandler);
                newPingThread(MyConfig.getLocalIp(),18889+count,nodeId,myMessageHandler,MyConfig.getTo().getInetSocketAddressV4());
                count++;
            }
        }
//        try {
//            Bootstrap bootstrap = getBootstrap(group, myEventHandler);
//
//            // 启动客户端，绑定本地端口
//            Channel channel = bootstrap.bind(MyConfig.getFromPort()).sync().channel();
//
//            //test
//
//            //test
//
//            // 构建自定义消息
//            MyKadPingMessage PingMsg = new MyKadPingMessage();
//
//            // 构建 UdpEvent
//            InetSocketAddress targetAddress = new InetSocketAddress(MyConfig.getToIp(), MyConfig.getToPort());
//            UdpEvent udpEvent = new UdpEvent(PingMsg, targetAddress);
//
//            // 获取 MessageHandler 实例并发送消息
//            MyMessageHandler myMessageHandler = (MyMessageHandler) channel.pipeline().last();
//            startSendingMessages(channel,myMessageHandler,udpEvent);
//
//            // 等待通道关闭
//            channel.closeFuture().await();
//        } finally {
//            group.shutdownGracefully();
//        }
    }

    public static Bootstrap getBootstrap(NioEventLoopGroup group, MyEventHandler myEventHandler,NodeId localTmpId,int fromPort) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch)
                            throws Exception {
                        ch.pipeline().addLast(TrafficStats.udp);
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                        ch.pipeline().addLast(new MyP2pPacketDecoder());
                        MyMessageHandler messageHandler = new MyMessageHandler(ch, myEventHandler,localTmpId,fromPort);
                        myEventHandler.setMessageHandler(messageHandler);
                        ch.pipeline().addLast(messageHandler);
                    }
                });
        return bootstrap;
    }

    public static void startSendingMessages(Channel channel, MyMessageHandler messageHandler, MyUdpEvent myUdpEvent) {
        new Thread(() -> {
            try {
                while (true) {
                    // 发送消息
                    //System.out.println("Sending message...");
                    messageHandler.accept(myUdpEvent);

                    // 设置发送间隔，避免过于频繁
                    Thread.sleep(10000); // 每隔10秒发送一次
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * 起一个新的线程，向目标端口反复发送Kad_ping消息，并回复对方端口发送的Kad_ping
     *
     * @param ip             使用的本地IP
     * @param localPort      使用的本地端口
     * @param nodeId         使用的本地nodeId
     * @param messageHandler 使用的消息处理器
     * @param targetAddress  目标端口的Socket地址
     */
    public static void newPingThread(String ip, int localPort,NodeId nodeId,MyMessageHandler messageHandler,InetSocketAddress targetAddress) {
        //MyEventHandler myEventHandler=new MyEventHandler(null);
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            //Bootstrap bootstrap = getBootstrap(group, myEventHandler);

            // 启动客户端，绑定本地端口

            //MyMessageHandler myMessageHandler = (MyMessageHandler) channel.pipeline().last();
            MyKadPingMessage PingMsg = new MyKadPingMessage(nodeId, localPort);
            MyUdpEvent myUdpEvent = new MyUdpEvent(PingMsg, targetAddress);

            new Thread(() -> {
                try {
                    while (true) {
                        // 发送消息
                        int distance = NodeEntry.distance(MyConfig.getRemoteId(),Tool.toPrimitive(nodeId.getNodeId()));
                        System.out.println("Sending message... with port "+localPort+" with target bucket: "+ NodeTableTest.getBucketId(distance));
                        byte[] nodeBytes = Tool.toPrimitive(nodeId.getNodeId()) ;
                        System.out.println("nodeId: "+nodeBytes[0]+" "+nodeBytes[1]+" "+nodeBytes[2]+" "+nodeBytes[3]);
                        messageHandler.accept(myUdpEvent);

                        // 设置发送间隔，避免过于频繁
                        Thread.sleep(5000); // 每隔5秒发送一次
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
    } finally {
            group.shutdownGracefully();
        }
        }
}
