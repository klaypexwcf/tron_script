package test;

import myDiscover.MyConfig;
import org.tron.p2p.discover.Node;

public class NodeTest {
    public static void main(String[] args) {
        MyConfig.init();
        Node node = MyConfig.getFrom();
        System.out.println(node.getInetSocketAddressV4().getAddress().getHostAddress());
    }
}
