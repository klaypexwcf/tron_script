package myDiscover.table;

import lombok.Getter;

@Getter
public class NodeIdTable {
    private final NodeIdBucket[] nodeIdTable = new NodeIdBucket[17];

    public void init(NodeId nodeId){
        //boolean[] bitNodeId = Tool.byteArrayToBitArray(nodeId.getNodeId());
        //Random random = new Random();
        for (int i =0; i<17;i++){
            NodeIdBucket nodeIdBucket = new NodeIdBucket();
            nodeIdBucket.init(nodeId,i);
            this.nodeIdTable[i]=nodeIdBucket;
        }
        System.out.println("NodeIdTable initialized");
        for (NodeIdBucket bucket:this.nodeIdTable){
            System.out.println(bucket.myToString());
        }
    }
    public NodeId getNodeIdFromSeq(int seq){
        seq+=1;
        int row = seq/16;
        int col = seq%16;
        NodeIdBucket nodeIdBucket = new NodeIdBucket();

        if(seq!=272){
            nodeIdBucket = this.nodeIdTable[row];
        }
        else{
            nodeIdBucket = this.nodeIdTable[row-1];
        }
        return (col==0)?this.nodeIdTable[row-1].getNodeBucket()[15]:nodeIdBucket.getNodeBucket()[col-1];
    }
}
