package myConnection.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannel;
import myConnection.MyChannelManager;
import org.slf4j.MDC;
import org.tron.p2p.connection.business.upgrade.UpgradeController;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.utils.ByteArray;

import java.util.List;

@Slf4j()
public class MyMessageHandler extends ByteToMessageDecoder {
    /**
     * 该类的实例依附于channel存在
     */
    protected final MyChannel channel;

    public MyMessageHandler(MyChannel channel) {
        this.channel = channel;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        MDC.put("customFileName",ctx.channel().remoteAddress().toString());
        log.info("Channel active, {}, local address: {}", ctx.channel().remoteAddress(), ctx.channel().localAddress().toString());
        //System.out.println("Channel active, " + ctx.channel().remoteAddress());
        channel.setChannelHandlerContext(ctx);
        if (channel.isActive()) {
            //System.out.println("Channel IS active, " + ctx.channel().remoteAddress());
            if (channel.isDiscoveryMode()) {
                //System.out.println("\\u001B[31m Channel is discv mode, " + ctx.channel().remoteAddress()+"\\u001B[0m");
                channel.sendStatusMsg();
            } else {
                //System.out.println("Channel isn't discv mode, " + ctx.channel().remoteAddress());
                if (MyChannelManager.isInit()) {
                    MyChannelManager.getHandshakeService().startHandshake(channel);
                }
            }
        }
        //MDC.remove("customFileName");
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) {
        byte[] data = new byte[buffer.readableBytes()];
        buffer.readBytes(data);
        try {
            if (channel.isFinishHandshake()) {
                data = UpgradeController.decodeReceiveData(channel.getVersion(), data);
            }
        MyChannelManager.processMessage(channel, data);
        } catch (Exception e) {
            if (e instanceof P2pException) {
                P2pException pe = (P2pException) e;
                Connect.DisconnectReason disconnectReason;
                switch (pe.getType()) {
                    case EMPTY_MESSAGE:
                        disconnectReason = Connect.DisconnectReason.EMPTY_MESSAGE;
                        break;
                    case BAD_PROTOCOL:
                        disconnectReason = Connect.DisconnectReason.BAD_PROTOCOL;
                        break;
                    case NO_SUCH_MESSAGE:
                        disconnectReason = Connect.DisconnectReason.NO_SUCH_MESSAGE;
                        break;
                    case BAD_MESSAGE:
                    case PARSE_MESSAGE_FAILED:
                    case MESSAGE_WITH_WRONG_LENGTH:
                    case TYPE_ALREADY_REGISTERED:
                        disconnectReason = Connect.DisconnectReason.BAD_MESSAGE;
                        break;
                    default:
                        disconnectReason = Connect.DisconnectReason.UNKNOWN;
                }
                channel.sendP2PDisconnectMsg(disconnectReason);
            }
            channel.processException(e);
        } catch (Throwable t) {
            log.error("Decode message from {} failed, message:{}", channel.getRemoteInetSocketAddress(),
                    ByteArray.toHexString(data));
            throw t;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        channel.processException(cause);
    }
}
