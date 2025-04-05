package nodeCrawler.NodeTestConnection;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.Getter;
import myConnection.socket.MyPeerClient;
import myDiscover.Tool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static nodeCrawler.NodeTestConnection.MyP2pChannelInitializerForNodeTestConnection.NEW_NODE_ID;
public class MyNodeCrawlerConnector {
    private MyPeerClient myPeerClient;
    //本地IP
    @Getter
    private String localIp;

    public MyNodeCrawlerConnector(String localIp) {
        this.localIp = localIp;
    }


    public boolean init(){
        //MyConfig.init();
        myPeerClient = new MyPeerClient();
        myPeerClient.init();
        return true;
    }
    public void close(){
        System.out.println("closing connector");
        myPeerClient.close();
    }
    public byte[] nodeIsReachable(String remoteIp, int remotePort, byte[] localId, int localPort) throws InterruptedException {
        //System.out.println("enter nodeIsReachable with parameters: "+remoteIp+":"+remotePort+":"+localPort);
        ChannelFuture channelFuture = myPeerClient
                .testConnection(Tool.wrapNode(remoteIp, remotePort, localId), localIp, localPort, Tool.byteArrayToHexString(localId));

        AtomicReferenceArray<byte[]> nodeId= new AtomicReferenceArray<>(1);
        //System.out.println("nodeId is null? "+nodeId.get(0));
        CountDownLatch latch = new CountDownLatch(1);

        channelFuture.channel().closeFuture().addListener((ChannelFutureListener) future -> {
//            if (future.isSuccess()) {
//                System.out.println("channel "+future.channel().remoteAddress()+" successfully closed");
//            }
//            else{
//                System.out.println("channel "+future.channel().remoteAddress()+" failed");
//            }
//            if(future.channel().isActive()){
//                System.out.println("channel "+future.channel().remoteAddress()+" is active");
//            }
            byte[] tmpId = future.channel().attr(NEW_NODE_ID).get();
            if(tmpId != null){
                nodeId.set(0,tmpId);
            }
            //System.out.println("get res " + remoteIp+":"+success);
            latch.countDown(); // 通知主线程
        });

        channelFuture.channel().closeFuture().sync(); // 等待 channel 关闭
        latch.await(); // 等待监听器执行完成

        //System.out.println("return res " + remoteIp+":"+isSuccess.get());
        return nodeId.get(0);
    }


}
