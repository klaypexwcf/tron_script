package nodeCrawler.Test;

import myDiscover.Tool;
import nodeCrawler.NodeTestConnection.MyNodeCrawlerConnector;

public class NodeConnectorTest {
    public static void main(String[] args) throws InterruptedException {
        MyNodeCrawlerConnector nodeConnector = new MyNodeCrawlerConnector("10.21.213.106");
        nodeConnector.init();
//        for (int i = 0; i < 2; i++) {
//            boolean tmp1 = nodeConnector.nodeIsReachable("35.126.107.76"
//                    ,18888, Tool.generateRandomNodeId(),14586);
//            System.out.println(tmp1);
//        }
        byte[] tmp = nodeConnector.nodeIsReachable("100.27.171.62"
                ,18888, Tool.generateRandomNodeId(),14585);
        if (tmp!=null) {
            System.out.println("node online"+tmp);
        }
        else{
            System.out.println("node offline");
        }
        nodeConnector.close();
        System.out.println("test stopped");
        System.out.println();
    }
}
