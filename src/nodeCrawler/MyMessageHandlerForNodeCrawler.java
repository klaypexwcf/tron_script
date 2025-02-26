package nodeCrawler;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;
import myDiscover.MyUdpEvent;
import myDiscover.table.NodeId;

import java.net.InetSocketAddress;
import java.util.function.Consumer;
@Slf4j(topic = "nodeCrawler")
public class MyMessageHandlerForNodeCrawler extends SimpleChannelInboundHandler<MyUdpEvent> implements Consumer<MyUdpEvent> {
    private final Channel channel;
    private final MyEventHandlerForNodeCrawler myEventHandler;
    private final NodeId localAttackId;
    private final int fromPort;

    public MyMessageHandlerForNodeCrawler(NioDatagramChannel channel, MyEventHandlerForNodeCrawler eventHandler, NodeId localAttackId, int fromPort) {
        this.channel = channel;
        this.myEventHandler = eventHandler;
        this.localAttackId = localAttackId;
        this.fromPort = fromPort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        myEventHandler.channelActivated();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, MyUdpEvent myUdpEvent) {
        System.out.println("Rcv udp msg type " + myUdpEvent.getMessage().getType() +
                ", len " + myUdpEvent.getMessage().getSendData().length +
                " from " + myUdpEvent.getAddress());
        myEventHandler.handleEvent(myUdpEvent,localAttackId,fromPort);

    }

    @Override
    public void accept(MyUdpEvent myUdpEvent) {
        System.out.println("Send udp msg type " + myUdpEvent.getMessage().getType() +
                ", len " + myUdpEvent.getMessage().getSendData().length +
                " to " + myUdpEvent.getAddress());
        InetSocketAddress address = myUdpEvent.getAddress();
        //System.out.println("msg: "+ myUdpEvent.getMessage().toString());
        sendPacket(myUdpEvent.getMessage().getSendData(), address);
    }

    void sendPacket(byte[] wire, InetSocketAddress address) {
        DatagramPacket packet = new DatagramPacket(Unpooled.copiedBuffer(wire), address);
        //System.out.println("wire: "+ Arrays.toString(wire));
        channel.writeAndFlush(packet).addListener(future -> {
            if (future.isSuccess()) {
                log.debug("send packet success");
            }
            else {
                Throwable cause = future.cause();
                log.debug("send packet failed: ", cause);
            }
        });

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("Exception caught in udp message handler, " + ctx.channel().remoteAddress() + " " + cause.getMessage());
        log.error("Exception caught in udp message handler {}",ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
