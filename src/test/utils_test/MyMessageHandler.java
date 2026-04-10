package test.utils_test;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;


import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class MyMessageHandler extends SimpleChannelInboundHandler<MyUdpEvent>
        implements Consumer<MyUdpEvent> {

    private final Channel channel;
    private final MyEventHandler eventHandler;

    public MyMessageHandler(NioDatagramChannel channel, MyEventHandler eventHandler) {
        this.channel = channel;
        this.eventHandler = eventHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MyUdpEvent udpEvent) {
        eventHandler.handleEvent(udpEvent);
    }

    @Override
    public void accept(MyUdpEvent udpEvent) {
        sendPacket(udpEvent.getMessage().getSendData(), udpEvent.getAddress());
    }

    private void sendPacket(byte[] wire, InetSocketAddress address) {
        DatagramPacket packet = new DatagramPacket(Unpooled.copiedBuffer(wire), address);
        channel.writeAndFlush(packet);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}