package myDiscover.table;

import myDiscover.Tool;

import java.util.Arrays;

public class NodeId {
    private Byte[] nodeId;

    public NodeId(Byte[] nodeId) {
        this.nodeId = nodeId;
    }

    public Byte[] getNodeId() {
        return nodeId;
    }

    public void setNodeId(Byte[] nodeId) {
        this.nodeId = nodeId;
    }

    public byte[] getLowerBytesId(){return Tool.toPrimitive(this.nodeId);}
    public String getHexId(){return Tool.byteArrayToHexString(getLowerBytesId());}
    public void changeIdByOneBit(){
        nodeId[0] = (byte) (nodeId[0] ^ 1);
    }
    @Override
    public String toString() {
        return "NodeId{" +
                "nodeId=" + Arrays.toString(nodeId) +
                '}';
    }
}
