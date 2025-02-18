package test;

import myDiscover.*;
import myDiscover.table.NodeId;
import myDiscover.table.NodeIdBucket;
import myDiscover.table.NodeIdTable;
import org.tron.p2p.discover.protocol.kad.table.NodeEntry;

public class NodeTableTest {
    public static void main(String[] args) {
        NodeIdTable nodeIdTable1 = new NodeIdTable();
        Byte[] byteId = Tool.toByteArray(MyConfig.getRemoteId());
        nodeIdTable1.init(new NodeId(byteId));
        int count =0;

        for (NodeIdBucket nodeIdBucket : nodeIdTable1.getNodeIdTable()) {
            for (NodeId nodeId : nodeIdBucket.getNodeBucket()) {
                int distance;
                distance = NodeEntry.distance(MyConfig.getRemoteId(), Tool.toPrimitive(nodeId.getNodeId()));
                if(getBucketId(distance)==7){
                    System.out.println("ee");
                }
                System.out.println(getBucketId(distance));
            }
            Byte[] tmpBytes = (nodeIdBucket.getNodeBucket())[0].getNodeId();
            int distance =NodeEntry.distance(MyConfig.getRemoteId(),Tool.toPrimitive(tmpBytes));
            System.out.println("bucket"+count+" have distance:"+ distance+" with bucketId:"+getBucketId(distance));
            count++;
        }


    }
    public static int getBucketId ( int dis){
        int id = dis - 1;
        return Math.max(id, 0);
    }

}

