package nodeCrawler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import myDiscover.MyConfig;
import myDiscover.MyP2pPacketDecoder;
import myDiscover.MyUdpEvent;
import myDiscover.Tool;
import myDiscover.message.MyFindNodeMessage;
import myDiscover.table.NodeId;
import nodeCrawler.NodeTestConnection.NodeOnlineUpdater;
import org.tron.p2p.discover.Node;
import org.tron.p2p.stats.TrafficStats;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j(topic = "nodeCrawler")
public class NodeCrawler {
    public final NodeCrawlerDb nodeCrawlerDb = new NodeCrawlerDb();

    public NodeId nodeId;
    public Node myNodeForUDP;
    public Node myPublicNode;
    public int fromPortForUDP =18888;
    /**
     * 使用ifconfig时显示的IP地址
     */
    public static String localIp="187.124.231.27";
    /**
     * 机器的公网IP
     */
    public static String publicLocalIp= "187.124.231.27";
    //是否启动节点爬取
    public static boolean findNewNodes=true;
    //是否启动节点存活性探测
    public static boolean detectOnline=true;




    private static final int MAX_IDLE_TIMES =10;
    private int idleTimes= 0;
    {
        byte[] localId =Tool.generateRandomNodeId();
        nodeId = new NodeId(Tool.toByteArray(localId));
        myNodeForUDP = new Node(Tool.toPrimitive(nodeId.getNodeId()), localIp,"", fromPortForUDP);
        myPublicNode = new Node(Tool.toPrimitive(nodeId.getNodeId()), publicLocalIp,"", fromPortForUDP);
    }

    public Connection getConnection() {
        return nodeCrawlerDb.getConnection();
    }

    public static void main(String[] args)  {
        System.setProperty("logback.configurationFile","src/nodeCrawler/resources/logback.xml");
        NodeCrawler nc = new NodeCrawler();
        if (findNewNodes) {
            try {
                MyMessageHandlerForNodeCrawler myMessageHandler = nc.neighborsListeningWork();
                nc.startSendWork(myMessageHandler);
                System.out.println("crawler instance working");
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (detectOnline) {
            NodeOnlineUpdater nodeOnlineUpdater = new NodeOnlineUpdater(localIp);
            nodeOnlineUpdater.startOnlineUpdater();
            System.out.println("node online updater working");
        }
        try {
            while(true){
                Thread.sleep(1_000*60);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 选择数据库中节点，依次发送FindNode消息
     * @throws SQLException e
     */
    public void startSendWork(MyMessageHandlerForNodeCrawler myMessageHandler) throws SQLException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            // 在单独的线程中执行该方法

            try(Connection conn = nodeCrawlerDb.getConnection()) {
                updateDbSize(conn);
                MyNodeInfoUtil myNodeInfo;
                while(true){
                    if (nodeCrawlerDb.dbCurrentCursorLineNum > nodeCrawlerDb.dbOldCursorLineNUm) {
                        idleTimes=0;
                        for(int i = nodeCrawlerDb.dbOldCursorLineNUm +1; i<= nodeCrawlerDb.dbCurrentCursorLineNum; i++) {
                            myNodeInfo = queryNodeInfoByCreateTimeOrder(i, conn);
                            if(myNodeInfo==null) {
                                throw new SQLException("get line info failed");
                            }
                            else if (myNodeInfo.ipv4==null||myNodeInfo.nodeId==null) {
                                System.out.println("line "+i+" has insufficient data");
                                log.error("line {} has insufficient data", i);
                                //跳过这一条数据，转到下一条数据
                                continue;
                            }
                            sendFindNodeWithRandomDstId(myNodeInfo.ipv4, myNodeInfo.udpPort,myMessageHandler);
                        }
                    }
                    else{
                        idleTimes++;
                        System.out.println("no new row in table, wait for 5s");
                        Thread.sleep(5_000);
                    }
                    if (idleTimes >= MAX_IDLE_TIMES) {
                        //如果长期闲置，那么从头开始再发送一次find_node
                        nodeCrawlerDb.resetCursorLineNum();
                    }
                    updateDbSize(conn);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.shutdown();

    }
    public void updateDbSize(Connection conn) throws SQLException {
        nodeCrawlerDb.updateTableSize(conn);
    }
    public MyNodeInfoUtil queryNodeInfoByCreateTimeOrder(int lineNum, Connection conn) throws SQLException {
        return nodeCrawlerDb.queryNodeInfoByCreateTimeOrder(lineNum, conn);
    }
    @Deprecated
    public void sendFindNode(String dstIp,int dstPort, NodeId dstNodeId,MyMessageHandlerForNodeCrawler myMessageHandler)  {
        dstNodeId.changeIdByOneBit();
        //注意，这里nodeId发生了一位的变化！
        MyFindNodeMessage myFindNodeMessage =new MyFindNodeMessage(myNodeForUDP,dstNodeId.getLowerBytesId());
        MyUdpEvent myUdpEvent = new MyUdpEvent(myFindNodeMessage,new InetSocketAddress(dstIp,dstPort));
        myMessageHandler.accept(myUdpEvent);
        System.out.println("sent findNode to "+dstIp+":"+dstPort);
    }
    public void sendFindNodeWithRandomDstId(String dstIp,int dstPort,MyMessageHandlerForNodeCrawler myMessageHandler){
        byte[] randomId = Tool.generateRandomNodeId();
        NodeId dstNodeId = new NodeId(Tool.toByteArray(randomId));
        MyFindNodeMessage myFindNodeMessage =new MyFindNodeMessage(myPublicNode,dstNodeId.getLowerBytesId());
        MyUdpEvent myUdpEvent = new MyUdpEvent(myFindNodeMessage,new InetSocketAddress(dstIp,dstPort));
        myMessageHandler.accept(myUdpEvent);
        System.out.println("sent findNode to "+dstIp+":"+dstPort);
    }
    public MyMessageHandlerForNodeCrawler neighborsListeningWork() throws SQLException, InterruptedException {
        Byte[] byteId = Tool.toByteArray(MyConfig.getRemoteId());
        NodeId nodeId = new NodeId(byteId);
        MyEventHandlerForNodeCrawler myEventHandler=new MyEventHandlerForNodeCrawler(null);
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = getBootstrapForNodeCrawler(group, myEventHandler,nodeId, fromPortForUDP);
        //在局域网运行时，需要改这里的IP
        Channel channel =bootstrap.bind( new InetSocketAddress(localIp, fromPortForUDP)).sync().channel();
        addShutdownHook(channel,group);
        //channel.closeFuture().sync();
        return (MyMessageHandlerForNodeCrawler) channel.pipeline().last();
    }
    public void addShutdownHook(Channel channel,NioEventLoopGroup group) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // 在程序终止时优雅地关闭 channel 和 NioEventLoopGroup
                if (channel != null) {
                    channel.close().sync();  // 关闭 channel
                }
                if (group != null) {
                    group.shutdownGracefully().sync();  // 关闭事件循环组
                }
                System.out.println("Shutting down gracefully...");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
    public static Bootstrap getBootstrapForNodeCrawler(NioEventLoopGroup group, MyEventHandlerForNodeCrawler myEventHandler,NodeId localTmpId,int fromPort){
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch) {
                        ch.pipeline().addLast(TrafficStats.udp);
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                        ch.pipeline().addLast(new MyP2pPacketDecoder());
                        MyMessageHandlerForNodeCrawler messageHandler = new MyMessageHandlerForNodeCrawler(ch, myEventHandler,localTmpId,fromPort);
                        myEventHandler.setMessageHandler(messageHandler);
                        ch.pipeline().addLast(messageHandler);
                    }
                });
        return bootstrap;
    }



}
