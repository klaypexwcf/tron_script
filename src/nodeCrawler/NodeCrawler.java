package nodeCrawler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import myDiscover.MyConfig;
import myDiscover.MyP2pPacketDecoder;
import myDiscover.MyUdpEvent;
import myDiscover.Tool;
import myDiscover.message.MyFindNodeMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.Node;
import org.tron.p2p.stats.TrafficStats;

import java.net.InetSocketAddress;
import java.sql.*;

public class NodeCrawler {
    public final NodeCrawlerDb nodeCrawlerDb = new NodeCrawlerDb();

    public NodeId nodeId;
    public Node myNode;
    public int fromPort=18888;
    {
         byte[] localId ={-50, -117, 20, 19, -71, 85, -16, 57, 5, -59, -22, -79, -103, -118, -12, 54, 92, -77, 13, 71, 35, 75, 41, -91, -101, -38, -76, -47, -106, 116, -1, 7, -43, 23, -63, 27, 122, 107, -67, 127, -44, 67, 79, 43, 55, 117, -46, 18, 118, 5, 17, 39, -90, 52, 50, 12, 62, 11, 47, 29, -67, -75, 68, -14};
        nodeId = new NodeId(Tool.toByteArray(localId));
        myNode= new Node(Tool.toPrimitive(nodeId.getNodeId()), MyConfig.getLocalIp(),"",fromPort);
    }

    public Connection getConnection() throws SQLException {
        return nodeCrawlerDb.getConnection();
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        NodeCrawler nc = new NodeCrawler();
        MyMessageHandlerForNodeCrawler myMessageHandler = nc.neighborsListeningWork();
        nc.startSendWork(myMessageHandler);
        System.out.println("crawler instance working");
    }

    /**
     * 选择数据库中节点，依次发送FindNode消息
     * @throws SQLException e
     */
    public void startSendWork(MyMessageHandlerForNodeCrawler myMessageHandler) throws SQLException, InterruptedException {
        Connection conn = nodeCrawlerDb.getConnection();
        PreparedStatement ps = conn.prepareStatement(nodeCrawlerDb.sizeQuery);
        ResultSet rs = ps.executeQuery();
        if(rs.next()) {
            nodeCrawlerDb.dbCurrentSize = rs.getInt(1);
            System.out.println("table have "+ nodeCrawlerDb.dbCurrentSize +" rows for now");
        }
        else {
            System.out.println("get table size failed");
            throw new SQLException("get table size failed");
        }
        MyNodeInfoUtil myNodeInfo;
        while(true){
            if (nodeCrawlerDb.dbCurrentSize > nodeCrawlerDb.dbOldSize) {
                for(int i = nodeCrawlerDb.dbOldSize +1; i<= nodeCrawlerDb.dbCurrentSize; i++) {
                   myNodeInfo = nodeCrawlerDb.queryNodeInfoById(i, conn);
                   if(myNodeInfo==null) {
                       throw new SQLException("get line info failed");
                   }
                   else if (myNodeInfo.ipv4==null||myNodeInfo.nodeId==null) {
                       System.out.println("line "+i+" has insufficient data");
                       break;
                   }
                   sendFindNode(myNodeInfo.ipv4, myNodeInfo.udpPort, myNodeInfo.nodeId,myMessageHandler);
                }
            }
            else{
                System.out.println("no new row in table, wait for 5s");
                Thread.sleep(5_000);
            }
            nodeCrawlerDb.updateDbSize(conn);
        }


    }
    public void updateDbSize(Connection conn) throws SQLException {
        nodeCrawlerDb.updateDbSize(conn);
    }
    public MyNodeInfoUtil queryNodeInfoById(int id, Connection conn) throws SQLException {
        return nodeCrawlerDb.queryNodeInfoById(id, conn);
    }
    public void sendFindNode(String dstIp,int dstPort, NodeId dstNodeId,MyMessageHandlerForNodeCrawler myMessageHandler)  {
        MyFindNodeMessage myFindNodeMessage =new MyFindNodeMessage(myNode,dstNodeId.getLowerBytesId());
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
        Channel channel =bootstrap.bind( new InetSocketAddress(MyConfig.getLocalIp(),fromPort)).sync().channel();
        return (MyMessageHandlerForNodeCrawler) channel.pipeline().last();
    }

    public static Bootstrap getBootstrapForNodeCrawler(NioEventLoopGroup group, MyEventHandlerForNodeCrawler myEventHandler,NodeId localTmpId,int fromPort){
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
                        MyMessageHandlerForNodeCrawler messageHandler = new MyMessageHandlerForNodeCrawler(ch, myEventHandler,localTmpId,fromPort);
                        myEventHandler.setMessageHandler(messageHandler);
                        ch.pipeline().addLast(messageHandler);
                    }
                });
        return bootstrap;
    }


}
