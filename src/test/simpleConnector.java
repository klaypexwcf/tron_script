package test;

import io.netty.channel.ChannelFuture;
import myConnection.MyChannelManager;
import myDiscover.MyConfig;
import org.slf4j.MDC;

public class simpleConnector {
    private static MyChannelManager myChannelManager;

    public static void main(String[] args) {

        String remoteIp= "";
        String localIp = "";

        if(args.length >0){
            remoteIp= args[0];
            localIp= args[1];
        }
        MDC.put("customFileName",remoteIp);
        MyChannelManager.init();
        MyConfig.setLocalIp(localIp);
        MyConfig.setToIp(remoteIp);
        MyConfig.init();
        ChannelFuture channelFuture = MyChannelManager.getPeerClient().connectAsync(MyConfig.wrapNode(remoteIp), false,null);
    }
}
