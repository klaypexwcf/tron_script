package myDiscover;

import lombok.Getter;

import java.util.Random;

@Getter
public class NodeIdTable {
    private final NodeIdBucket[] nodeIdTable = new NodeIdBucket[17];

    public void init(NodeId nodeId){
        boolean[] bitNodeId = Tool.byteArrayToBitArray(nodeId.getNodeId());
        Random random = new Random();
        for (int i =0; i<17;i++){
            NodeIdBucket nodeIdBucket = new NodeIdBucket();
            nodeIdBucket.init(nodeId,i);
            this.nodeIdTable[i]=nodeIdBucket;
        }
        System.out.println("NodeIdTable initialized");
        for (NodeIdBucket bucket:this.nodeIdTable){
            System.out.println("bucket"+bucket.toString());
        }
    }
}
