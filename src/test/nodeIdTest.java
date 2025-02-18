package test;

import myDiscover.MyConfig;
import myDiscover.Tool;

public class nodeIdTest {
    public static void main(String[] args) {
        System.out.println(Tool.byteArrayToHexString(MyConfig.getRemoteId()));
    }
}
