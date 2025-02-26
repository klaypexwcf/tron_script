package test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import myDiscover.MyConfig;
import myDiscover.MyUdpEvent;
import myDiscover.Tool;
import myDiscover.message.MyFindNodeMessage;
import myDiscover.table.NodeId;
import nodeCrawler.MyEventHandlerForNodeCrawler;
import nodeCrawler.MyMessageHandlerForNodeCrawler;
import nodeCrawler.NodeCrawler;
import org.tron.p2p.discover.Node;

import java.net.InetSocketAddress;
import java.sql.SQLException;

public class UDPMessageTest {
    public static void main(String[] args) throws SQLException, InterruptedException {
        Byte[] byteId = Tool.toByteArray(MyConfig.getRemoteId());
        NodeId nodeId = new NodeId(byteId);
        MyEventHandlerForNodeCrawler myEventHandler=new MyEventHandlerForNodeCrawler(null);
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = NodeCrawler.getBootstrapForNodeCrawler(group,myEventHandler,nodeId,18888);
        Channel channel =bootstrap.bind( new InetSocketAddress("10.21.213.106",18888)).sync().channel();
        MyMessageHandlerForNodeCrawler myMessageHandlerForNodeCrawler = (MyMessageHandlerForNodeCrawler) channel.pipeline().last();
        byte[] localId ={-50, -117, 20, 19, -71, 85, -16, 57, 5, -59, -22, -79, -103, -118, -12, 54, 92, -77, 13, 71, 35, 75, 41, -91, -101, -38, -76, -47, -106, 116, -1, 7, -43, 23, -63, 27, 122, 107, -67, 127, -44, 67, 79, 43, 55, 117, -46, 18, 118, 5, 17, 39, -90, 52, 50, 12, 62, 11, 47, 29, -67, -75, 68, -14};
        nodeId = new NodeId(Tool.toByteArray(localId));
        Node myNode= new Node(Tool.toPrimitive(nodeId.getNodeId()), MyConfig.getLocalIp(),"",18888);
        byte[] randomId = Tool.generateRandomNodeId();
        NodeId dstNodeId = new NodeId(Tool.toByteArray(randomId));
        MyFindNodeMessage myFindNodeMessage =new MyFindNodeMessage(myNode,dstNodeId.getLowerBytesId());
        MyUdpEvent myUdpEvent = new MyUdpEvent(myFindNodeMessage,new InetSocketAddress("35.223.231.3",18888));
        myMessageHandlerForNodeCrawler.accept(myUdpEvent);
    }
}
