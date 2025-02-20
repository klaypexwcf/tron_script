package nodeCrawler;

import lombok.Setter;
import myDiscover.MyUdpEvent;
import myDiscover.message.MyKadPongMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad.NeighborsMessage;

import java.sql.Connection;
import java.sql.SQLException;

public class MyEventHandlerForNodeCrawler {
    @Setter
    private MyMessageHandlerForNodeCrawler messageHandler;
    private final NodeCrawlerDb nodeCrawlerDb=new NodeCrawlerDb();
    private final Connection conn= nodeCrawlerDb.getConnection();

    public MyEventHandlerForNodeCrawler(MyMessageHandlerForNodeCrawler messageHandler) throws SQLException {
        this.messageHandler = messageHandler;
    }
    public void handleEvent(MyUdpEvent myUdpEvent, NodeId localNodeId, int fromPort)  {
        // 处理接收到的事件
        System.out.println("Handling event: " + myUdpEvent.getMessage().getType().toString());
        Message msg = myUdpEvent.getMessage();
        if (msg.getType()== MessageType.KAD_PING){
            //回复收到的Kad_ping
            System.out.println("received msg type kad_ping");
            Message pongReply=new MyKadPongMessage(localNodeId,fromPort);
            MyUdpEvent replyEvent = new MyUdpEvent(pongReply, myUdpEvent.getAddress());
            messageHandler.accept(replyEvent);
        }
        else if (msg.getType()==MessageType.KAD_PONG) {
            //是Kad_pong则简单输出日志
            System.out.println("received msg type kad_pong");
        }
        else if (msg.getType()==MessageType.KAD_NEIGHBORS){
            System.out.println("received msg type kad_neighbors from "+myUdpEvent.getAddress());
            try {
                int num = processNeighborsMsg(msg,conn);
                System.out.println("insert "+num+" nodes info");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void channelActivated() {
        System.out.println("Channel activated");
    }

    private int processNeighborsMsg(Message msg,Connection conn) throws SQLException {
        return nodeCrawlerDb.nodeInfoBatchInsert((NeighborsMessage) msg,conn);
    }
}
