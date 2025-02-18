package myConnection;


import lombok.extern.slf4j.Slf4j;
import myConnection.higher.H_handshake.MyHHandshake;
import myConnection.higher.H_keepalive.MyHKeepAlive;
import myConnection.higher.MyHMessage.MyHDisconnectMessage;
import myConnection.higher.MyHMessage.MyHHelloMessage;
import myConnection.higher.MyHMessage.MyTronMessage;
import myDiscover.Tool;
import org.tron.core.exception.P2pException;
import org.tron.core.net.message.MessageTypes;
import org.tron.protos.Protocol;

import java.util.Set;


@Slf4j
public class MyHP2pEventHandlerImpl extends MyP2pEventHandler {

    //private MyHHandshake handshake;
    public MyHP2pEventHandlerImpl() {
        super();
    }

    @Override
    public void onConnect(MyChannel channel) {
        MyHHandshake.startHandshake(channel);
        super.onConnect(channel);
    }

    @Override
    public void onDisconnect(MyChannel channel) {
        super.onDisconnect(channel);
    }

    @Override
    public void onMessage(MyChannel channel, byte[] data) {
        super.onMessage(channel, data);
        //log.info("received msg from peer {}",channel.getInetSocketAddress());
        processMessage(channel,data);
    }
    private void processMessage(MyChannel channel, byte[] data) {
        long startTime = System.currentTimeMillis();
        MyTronMessage msg = null;
        MessageTypes type = null;
        try {
            msg = Tool.create(data);
            type = msg.getType();
            log.info("Receive message from peer: {}, {}", channel.getRemoteInetSocketAddress(), msg);
            switch (type) {
                case P2P_PING:
                case P2P_PONG:
                    MyHKeepAlive.processMessage(channel, msg);
                    break;
                case P2P_HELLO:
                    MyHHandshake.processHelloMessage(channel, (MyHHelloMessage) msg);
                    break;
                case P2P_DISCONNECT:
                    channel.setDisconnectReason(((MyHDisconnectMessage) msg).getReason().toString());
                    channel.close(((MyHDisconnectMessage) msg).getReason().toString());
                    log.info("received disconnect msg from channel {} with cause {}", channel.getRemoteInetSocketAddress(), ((MyHDisconnectMessage) msg).getReason());
                    break;
                case SYNC_BLOCK_CHAIN:
                    log.info("received SYNC_BLOCK_CHAIN from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //syncBlockChainMsgHandler.processMessage(peer, msg);
                    break;
                case BLOCK_CHAIN_INVENTORY:
                    log.info("received BLOCK_CHAIN_INVENTORY from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //chainInventoryMsgHandler.processMessage(peer, msg);
                    break;
                case INVENTORY:
                    log.info("received INVENTORY from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //inventoryMsgHandler.processMessage(peer, msg);
                    break;
                case FETCH_INV_DATA:
                    log.info("received FETCH_INV_DATA from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //fetchInvDataMsgHandler.processMessage(peer, msg);
                    break;
                case BLOCK:
                    log.info("received BLOCK from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //blockMsgHandler.processMessage(peer, msg);
                    break;
                case TRXS:
                    log.info("received TRXS from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //transactionsMsgHandler.processMessage(peer, msg);
                    break;
                case PBFT_COMMIT_MSG:
                    log.info("received PBFT_COMMIT_MSG from channel: {}, {}", channel.getRemoteInetSocketAddress(), msg);
                    //pbftDataSyncHandler.processMessage(peer, msg);
                    break;
                default:
                    throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, msg.getType().toString());
            }
        } catch (Exception e) {
            processException(channel, msg, e);
        }
    }
    @Override
    public Set<Byte> getMessageTypes() {
        return super.getMessageTypes();
    }
    public void processException(MyChannel channel,MyTronMessage msg,Exception ex){
        Protocol.ReasonCode code;
        if (ex instanceof P2pException) {
            P2pException.TypeEnum type = ((P2pException) ex).getType();
            switch (type) {
                case BAD_TRX:
                    code = Protocol.ReasonCode.BAD_TX;
                    break;
                case BAD_BLOCK:
                    code = Protocol.ReasonCode.BAD_BLOCK;
                    break;
                case NO_SUCH_MESSAGE:
                    code = Protocol.ReasonCode.NO_SUCH_MESSAGE;
                    break;
                case BAD_MESSAGE:
                    code = Protocol.ReasonCode.BAD_PROTOCOL;
                    break;
                case SYNC_FAILED:
                    code = Protocol.ReasonCode.SYNC_FAIL;
                    break;
                case UNLINK_BLOCK:
                    code = Protocol.ReasonCode.UNLINKABLE;
                    break;
                case DB_ITEM_NOT_FOUND:
                    code = Protocol.ReasonCode.FETCH_FAIL;
                    break;
                default:
                    code = Protocol.ReasonCode.UNKNOWN;
                    break;
            }
            if (type.equals(P2pException.TypeEnum.BAD_MESSAGE)) {
                log.error("Message from {} process failed, {} \n type: ({})",
                        channel.getRemoteInetSocketAddress(), msg, type, ex);
            } else {
                log.warn("Message from {} process failed, {} \n type: ({}), detail: {}",
                        channel.getRemoteInetSocketAddress(), msg, type, ex.getMessage());
            }
        } else {
            code = Protocol.ReasonCode.UNKNOWN;
            log.warn("Message from {} process failed, {}",
                    channel.getRemoteInetSocketAddress(), msg, ex);
        }
    }
}
