package central;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import sun.security.krb5.internal.crypto.Des;

class Msg{
    public String Source;
    public String Command;
    public String Type;
    public String Destination;

    public Msg(ZMsg inp) throws Exception
    {
        int size=inp.size();
        if(size==3){
            this.Source=inp.popString();
            this.Command=inp.popString();
            this.Type=inp.popString();
        }else if (size==4){
            this.Source=inp.popString();
            this.Destination=inp.popString();
            this.Command=inp.popString();
            this.Type=inp.popString();
        }else{//WTF?
            throw new Exception("unsupported message type: Size mismatch...");
        }
    }

    public void dump(){
        System.out.println("---------Dumping Message Content---------");
        System.out.println("Message Sender: "+Source);
        System.out.println("Message Destination: "+ Destination);
        System.out.println("Message Command: "+Command);
        System.out.println("Message Type: "+Type);
    }

}

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
