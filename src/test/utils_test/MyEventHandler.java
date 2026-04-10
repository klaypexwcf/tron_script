package test.utils_test;


import myDiscover.message.MyKadPongMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class MyEventHandler {

    private final NodeId localNodeId;
    private final int fromPort;
    private final byte[] targetId;
    private final MyNeighborsListener neighborsListener;

    private Consumer<MyUdpEvent> sender;

    public MyEventHandler(NodeId localNodeId,
                          int fromPort,
                          byte[] targetId,
                          MyNeighborsListener neighborsListener) {
        this.localNodeId = localNodeId;
        this.fromPort = fromPort;
        this.targetId = targetId;
        this.neighborsListener = neighborsListener;
    }

    public void setSender(Consumer<MyUdpEvent> sender) {
        this.sender = sender;
    }

    public void handleEvent(MyUdpEvent udpEvent) {
        Message msg = udpEvent.getMessage();
        MessageType type = msg.getType();

        if (type == MessageType.KAD_PING) {
            replyPong(udpEvent.getAddress());
            return;
        }

        if (type == MessageType.KAD_NEIGHBORS) {
            handleNeighbors(udpEvent);
        }
    }

    private void replyPong(InetSocketAddress address) {
        if (sender == null) {
            return;
        }
        Message pongReply = new MyKadPongMessage(localNodeId, fromPort);
        sender.accept(new MyUdpEvent(pongReply, address));
    }

    private void handleNeighbors(MyUdpEvent udpEvent) {
        List<Node> nodes = extractNodes(udpEvent.getMessage());
        boolean containsTarget = nodes.stream()
                .anyMatch(node -> Arrays.equals(node.getId(), targetId));

        if (neighborsListener != null) {
            neighborsListener.onResult(udpEvent.getAddress(), nodes, containsTarget);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Node> extractNodes(Message msg) {
        try {
            Method method = msg.getClass().getMethod("getNodes");
            Object value = method.invoke(msg);
            if (value instanceof List) {
                return (List<Node>) value;
            }
        } catch (Exception ignored) {
        }
        return Collections.emptyList();
    }
}