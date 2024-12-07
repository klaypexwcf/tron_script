package myDiscover;

import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;

public class MyEventHandler {


    private MyMessageHandler messageHandler;

    public MyEventHandler(MyMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }
    public void handleEvent(UdpEvent udpEvent) {
        // 处理接收到的事件
        System.out.println("Handling event: " + udpEvent.getMessage().getType().toString());
        Message msg = udpEvent.getMessage();
        if (msg.getType()== MessageType.KAD_PING){
            //回复收到的Kad_ping
            System.out.println("received msg type kad_ping"+System.currentTimeMillis());
            Message pongReply=new MyKadPongMessage();
            UdpEvent replyEvent = new UdpEvent(pongReply,udpEvent.getAddress());
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