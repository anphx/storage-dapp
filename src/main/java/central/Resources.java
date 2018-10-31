package central;

import org.zeromq.ZMQ;

public class Resources {
    public int ROUTER_DEALER_PORT;
    public int PUB_SUB_PORT;
    public String PUB_SUB_PROTOCOL;
    public String ROUTER_DEALER_PROTOCOL;


    private static Resources rs;
    private Resources(){
        PUB_SUB_PORT=5543;
        ROUTER_DEALER_PORT=5544;
        PUB_SUB_PROTOCOL="tcp";
        ROUTER_DEALER_PROTOCOL="tcp";
        //context=new
    }
    public static Resources getInstance(){
        if(rs==null){
            rs=new Resources();
        }
        return rs;
    }
    public static ZMQ.Context context;
}
