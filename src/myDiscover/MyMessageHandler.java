package myDiscover;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class MyMessageHandler extends SimpleChannelInboundHandler<UdpEvent> implements Consumer<UdpEvent> {
    private final Channel channel;
    private final MyEventHandler myEventHandler;

    public MyMessageHandler(NioDatagramChannel channel, MyEventHandler eventHandler) {
        this.channel = channel;
        this.myEventHandler = eventHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        myEventHandler.channelActivated();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, UdpEvent udpEvent) {
        System.out.println("Rcv udp msg type " + udpEvent.getMessage().getType() +
                ", len " + udpEvent.getMessage().getSendData().length +
                " from " + udpEvent.getAddress());
        myEventHandler.handleEvent(udpEvent);

    }

    @Override
    public void accept(UdpEvent udpEvent) {
        System.out.println("Send udp msg type " + udpEvent.getMessage().getType() +
                ", len " + udpEvent.getMessage().getSendData().length +
                " to " + udpEvent.getAddress());
        InetSocketAddress address = udpEvent.getAddress();
        sendPacket(udpEvent.getMessage().getSendData(), address);
    }

    void sendPacket(byte[] wire, InetSocketAddress address) {
        DatagramPacket packet = new DatagramPacket(Unpooled.copiedBuffer(wire), address);
        channel.writeAndFlush(packet).addListener(future -> {
            if (future.isSuccess()) {
                System.out.println("send success");
            }
            else {
                System.out.println("send failed");
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
        ctx.close();
    }
}