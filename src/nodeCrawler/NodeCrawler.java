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
    public Node myNode;
    public Node myPublicNode;
    public int fromPort=18888;
    //本地IP
    public static String localIp="10.2.8.6";
    public static String publicLocalIp="58.87.95.60";


    private static final int MAX_IDLE_TIMES =10;
    private int idleTimes= 0;
    {
        byte[] localId ={-50, -117, 20, 19, -71, 85, -16, 57, 5, -59, -22, -79, -103, -118, -12, 54, 92, -77, 13, 71, 35, 75, 41, -91, -101, -38, -76, -47, -106, 116, -1, 7, -43, 23, -63, 27, 122, 107, -67, 127, -44, 67, 79, 43, 55, 117, -46, 18, 118, 5, 17, 39, -90, 52, 50, 12, 62, 11, 47, 29, -67, -75, 68, -14};
        nodeId = new NodeId(Tool.toByteArray(localId));
        myNode= new Node(Tool.toPrimitive(nodeId.getNodeId()), localIp,"",fromPort);
        myPublicNode = new Node(Tool.toPrimitive(nodeId.getNodeId()), publicLocalIp,"",fromPort);
    }

    public Connection getConnection() throws SQLException {
        return nodeCrawlerDb.getConnection();
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        System.setProperty("logback.configurationFile","src/nodeCrawler/resources/logback.xml");
        NodeCrawler nc = new NodeCrawler();
        MyMessageHandlerForNodeCrawler myMessageHandler = nc.neighborsListeningWork();
        nc.startSendWork(myMessageHandler);
        System.out.println("crawler instance working");
        NodeOnlineUpdater nodeOnlineUpdater = new NodeOnlineUpdater(localIp);
        nodeOnlineUpdater.startOnlineUpdater();
        System.out.println("node online updater working");
        while(true){
            Thread.sleep(1000);
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
            Connection conn = nodeCrawlerDb.getConnection();
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
        MyFindNodeMessage myFindNodeMessage =new MyFindNodeMessage(myNode,dstNodeId.getLowerBytesId());
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
        Bootstrap bootstrap = getBootstrapForNodeCrawler(group, myEventHandler,nodeId,fromPort);
        //在局域网运行时，需要改这里的IP
        Channel channel =bootstrap.bind( new InetSocketAddress(localIp,fromPort)).sync().channel();
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
