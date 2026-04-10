package test.utils_test;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.exception.P2pException;

import java.util.List;

public class MyP2pPacketDecoder extends MessageToMessageDecoder<DatagramPacket> {
    private static final int MAX_SIZE = 2048;

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) {
        ByteBuf buf = packet.content();
        int length = buf.readableBytes();
        if (length <= 1 || length >= MAX_SIZE) {
            return;
        }

        byte[] encoded = new byte[length];
        buf.readBytes(encoded);

        try {
            out.add(new MyUdpEvent(Message.parse(encoded), packet.sender()));
        } catch (P2pException | InvalidProtocolBufferException ignored) {
        } catch (Exception ignored) {
        }
    }
}