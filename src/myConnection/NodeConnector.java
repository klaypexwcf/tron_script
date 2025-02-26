package myConnection;

import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;
import myDiscover.MyConfig;
import myDiscover.Tool;
import myDiscover.table.NodeId;
import myDiscover.table.NodeIdTable;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NodeConnector {
    private static List<ChannelFuture> futureList = new ArrayList<ChannelFuture>();
    public static void main(String[] args) throws InterruptedException {
        MyConfig.hp2pEventHandler=new MyHP2pEventHandlerImpl();
        String toIP="81.70.23.5";
        int toPort=18888;
        byte[] toId= MyConfig.getRemoteId();
        MyChannelManager.init();

        NodeIdTable nodeIdTable =new NodeIdTable();
        Byte[] byteId = Tool.toByteArray(toId);
        nodeIdTable.init(new NodeId(byteId));
        System.out.println("localIP: "+MyConfig.getLocalIp());
        for (int i=0; i<31;i++){
            Thread.sleep(100);
            byte[] localId=Tool.generateRandomNodeId();
            ChannelFuture channelFuture = MyChannelManager.getPeerClient().connectAsync(MyConfig.wrapNode(toIP,0),false,Tool.byteArrayToHexString(localId));
            futureList.add(channelFuture);
        }
    }
}
