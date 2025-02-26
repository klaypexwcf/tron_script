package nodeCrawler.NodeTestConnection;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannel;
import myConnection.MyChannelManager;
import myConnection.message.MyMessage;
import myConnection.socket.MyMessageHandler;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.connection.business.upgrade.UpgradeController;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.utils.ByteArray;

import java.util.List;
@Slf4j
public class MyMessageHandlerForNodeTestConnection extends MyMessageHandler {

    private final MyHandshakeServiceForNodeTestConnection myHandshakeServiceForNodeTestConnection;

    public MyMessageHandlerForNodeTestConnection(MyChannel channel, MyHandshakeServiceForNodeTestConnection myHandshakeServiceForNodeTestConnection) {
        super(channel);
        this.myHandshakeServiceForNodeTestConnection = myHandshakeServiceForNodeTestConnection;
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("channelActive for "+ctx.channel().remoteAddress());
        channel.setChannelHandlerContext(ctx);
        if(channel.isActive()){
            myHandshakeServiceForNodeTestConnection.startHandshake(channel);
        }
    }
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) {
        byte[] data = new byte[buffer.readableBytes()];
        buffer.readBytes(data);
        try {
            if (channel.isFinishHandshake()) {
                data = UpgradeController.decodeReceiveData(channel.getVersion(), data);
            }
        processMessage(channel, data);
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
    protected void processMessage(MyChannel channel, byte[] data)throws P2pException{
        if (data[0] >= 0) {
            handMessage(channel, data);
            return;
        }
        MyMessage message = MyMessage.parse(data);

        System.out.println("receive msg from channel "+channel.getRemoteInetSocketAddress()+"type: "+message);
        switch (message.getType()) {
            case KEEP_ALIVE_PING:

                log.info("received KEEP_ALIVE_PING from {}",channel.getRemoteInetSocketAddress());
                //keepAliveService.processMessage(channel, message);
                break;
            case KEEP_ALIVE_PONG:
                log.info("received KEEP_ALIVE_PONG from {}",channel.getRemoteInetSocketAddress());
                //keepAliveService.processMessage(channel, message);
                break;
            case HANDSHAKE_HELLO:
                log.info("received HANDSHAKE_HELLO from {}, {}",channel.getRemoteInetSocketAddress(), message);
                myHandshakeServiceForNodeTestConnection.processMessage(channel, message);
                break;
            case STATUS:
                log.info("received STATUS from {}, {}",channel.getRemoteInetSocketAddress(),message);
                //nodeDetectService.processMessage(channel, message);
                break;
            case DISCONNECT:
                log.info("received DISCONNECT from {}, reason: {}",channel.getRemoteInetSocketAddress(), message);
                channel.setDisconnectReason(message.toString());
                channel.close(message.toString());
                break;
            default:
                throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, "type:" + data[0]);
        }

    }
    protected void handMessage(MyChannel channel, byte[] data)throws P2pException{
        if (channel.isDiscoveryMode()) {
            channel.sendP2PDisconnectMsg(Connect.DisconnectReason.DISCOVER_MODE);
            channel.getCtx().close();
            return;
        }
        if(!channel.isFinishHandshake()){
            channel.setFinishHandshake(true);
            DisconnectCode code =DisconnectCode.TOO_MANY_PEERS;
            Connect.DisconnectReason disconnectReason = MyChannelManager.getDisconnectReason(code);
            channel.sendP2PDisconnectMsg(disconnectReason);
            channel.getCtx().close();
            return;
        }
    }
}
