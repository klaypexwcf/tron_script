package myDiscover;

import myDiscover.message.MyKadPongMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;

public class MyEventHandler {


    private MyMessageHandler messageHandler;

    public MyEventHandler() {
    }

    public MyEventHandler(MyMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }
    public void handleEvent(MyUdpEvent myUdpEvent, NodeId localNodeId, int fromPort) {
        // 处理接收到的事件
        System.out.println("Handling event: " + myUdpEvent.getMessage().getType().toString());
        Message msg = myUdpEvent.getMessage();
        if (msg.getType()== MessageType.KAD_PING){
            //回复收到的Kad_ping
            System.out.println("received msg type kad_ping"+System.currentTimeMillis());
            Message pongReply=new MyKadPongMessage(localNodeId,fromPort);
            MyUdpEvent replyEvent = new MyUdpEvent(pongReply, myUdpEvent.getAddress());
            messageHandler.accept(replyEvent);
        }
        else if (msg.getType()==MessageType.KAD_PONG) {
            //是Kad_pong则简单输出日志
            System.out.println("received msg type kad_pong");
        }

    }
    public void setMessageHandler(MyMessageHandler myMessageHandler){
        this.messageHandler=myMessageHandler;
    }

    public void channelActivated() {
        System.out.println("Channel activated");
    }
}