package test;

import myDiscover.MyConfig;
import myDiscover.table.NodeId;
import myDiscover.Tool;
import org.tron.p2p.discover.protocol.kad.table.NodeEntry;

import java.util.Random;

public class arrayCopyTest {
    public static void main(String[] args) {
        Random random = new Random();
        byte[] lowerId=MyConfig.getRemoteId();
        boolean[] bitNodeId = Tool.byteArrayToBitArray(Tool.toByteArray(lowerId));
        boolean[] tmpBitNodeId = new boolean[512];
        Tool.copyFirstIBits(bitNodeId,tmpBitNodeId,5);
        tmpBitNodeId[5] = !bitNodeId[5];
        for (int k=6;k<512;k++){
            tmpBitNodeId[k]=random.nextBoolean();
        }
        Byte[] tmpNodeId = Tool.bitArrayToByteArray(tmpBitNodeId);
        NodeId newNodeId = new NodeId(tmpNodeId);
        int distance = NodeEntry.distance(Tool.toPrimitive(newNodeId.getNodeId()),MyConfig.getRemoteId());
        System.out.println(getBucketId(distance)+" "+distance);
    }
    public static int getBucketId ( int dis){
        int id = dis - 1;
        return Math.max(id, 0);
    }
}
