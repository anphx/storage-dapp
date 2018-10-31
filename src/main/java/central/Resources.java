package central;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import sun.security.krb5.internal.crypto.Des;
//class ZFrameMore extends ZFrame{
//    public ZFrameMore()
//    {
//        ZMQ.SNDMORE
//        super.MO
//    }
//}
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
            inp.dump();
            //throw new Exception("unsupported message type: Size mismatch...");
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


    public static ZMsg getQueryMessage(byte[] q){
        ZMsg outgoing=ZMsg.newStringMsg("Q");
        outgoing.push(q);
        return outgoing;
        //outgoing.send(dealer);
    }
    public static ZMsg getAddMessage(byte[] q){

        //ZMsg outgoing=ZMsg.newStringMsg("A");
        ZMsg outgoing= new ZMsg();
        //outgoing.add(new ZFrame("A"));

        //ZFrame zf = new ZFrame("k");
        outgoing.add(q);
        outgoing.add(new ZFrame("A"));
        return outgoing;
        //outgoing.send(dealer);

    }
    public static ZMsg getResponseMessage(byte[] resp, byte[] ClusterAddress){
        ZMsg outgoing=ZMsg.newStringMsg("R");
        outgoing.push("message");
        outgoing.push("destination address");
        return outgoing;
        //outgoing.send(dealer);

    }
    public static ZMsg getJoinMessage(byte[] Clusteraddress){
        ZMsg outgoing=ZMsg.newStringMsg("J");
        outgoing.push(Clusteraddress);
        return outgoing;
        //outgoing.send(dealer);
    }

}
