package myDiscover.table;

import myDiscover.MyConfig;
import myDiscover.Tool;
import org.tron.p2p.discover.protocol.kad.table.NodeEntry;
import test.NodeTableTest;

import java.util.Arrays;
import java.util.Random;

public class NodeIdBucket {


    private NodeId[] nodeIdBucket = new NodeId[16];
    public String myToString(){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i<nodeIdBucket.length; i++){
            boolean[] bits = Tool.byteArrayToBitArray(nodeIdBucket[i].getNodeId());
            sb.append(booleanArrayToBinaryString(bits)+"\n");
        }
        return sb.toString();
    }
    public static String booleanArrayToBinaryString(boolean[] boolArray) {
        if (boolArray == null || boolArray.length == 0) {
            return "";
        }

        StringBuilder binaryString = new StringBuilder();

        for (boolean b : boolArray) {
            binaryString.append(b ? '1' : '0');
        }

        return binaryString.toString();
    }

    public void init(NodeId nodeId, int distance) {
        boolean[] bitNodeId = Tool.byteArrayToBitArray(nodeId.getNodeId());
        Random random = new Random();
        for (int j = 0; j < 16; j++){
            boolean[] tmpBitNodeId = new boolean[512];
            if (!(distance<1)) {
                Tool.copyFirstIBits(bitNodeId,tmpBitNodeId,distance);
            }
            tmpBitNodeId[distance] = !bitNodeId[distance];
            for (int k=distance+1;k<512;k++){
                tmpBitNodeId[k]=random.nextBoolean();
            }
            Byte[] tmpNodeId = Tool.bitArrayToByteArray(tmpBitNodeId);
            NodeId newNodeId = new NodeId(tmpNodeId);
            //newNodeId.setNodeId(tmpNodeId);
            this.nodeIdBucket[j]=newNodeId;
        }
        for(int i=0;i<nodeIdBucket.length;i++){
            Byte[] tmpBytes = (nodeIdBucket[i].getNodeId());
            int distance1 = NodeEntry.distance(MyConfig.getRemoteId(),Tool.toPrimitive(tmpBytes));
            System.out.println("Node "+i+" have distance:"+ distance1+" with bucketId:"+ NodeTableTest.getBucketId(distance1));
        }
    }

    public NodeId[] getNodeBucket() {
        return nodeIdBucket;
    }
    @Override
    public String toString() {
        return "NodeIdBucket{" +
                "nodeIdBucket=" + Arrays.toString(nodeIdBucket) +
                '}';
    }
}
