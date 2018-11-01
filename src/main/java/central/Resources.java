package central;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

//class ZFrameMore extends ZFrame{
//    public ZFrameMore()
//    {
//        ZMQ.SNDMORE
//        super.MO
//    }
//}
//class Msg {
//    public String Source;
//    public String Command;
//    public String Type;
//    public String Destination;
//
//    public Msg(ZMsg inp) throws Exception {
//        int size = inp.size();
//        if (size == 3) {
//            this.Source = inp.popString();
//            this.Command = inp.popString();
//            this.Type = inp.popString();
//        } else if (size == 4) {
//            this.Source = inp.popString();
//            this.Destination = inp.popString();
//            this.Command = inp.popString();
//            this.Type = inp.popString();
//        } else {//WTF?
//            inp.dump();
//            //throw new Exception("unsupported message type: Size mismatch...");
//        }
//    }
//
//
//    public void dump() {
//        System.out.println("---------Dumping Msg Content---------");
//        System.out.println("Msg Sender: " + Source);
//        System.out.println("Msg Destination: " + Destination);
//        System.out.println("Msg Command: " + Command);
//        System.out.println("Msg Type: " + Type);
//    }
//
//}

public class Resources {
    public static ZMQ.Context context;
    private static Resources rs;
    public int ROUTER_DEALER_PORT;
    public int PUB_SUB_PORT;
    public String PUB_SUB_PROTOCOL;
    public String ROUTER_DEALER_PROTOCOL;

    private Resources() {
        PUB_SUB_PORT = 5543;
        ROUTER_DEALER_PORT = 5544;
        PUB_SUB_PROTOCOL = "tcp";
        ROUTER_DEALER_PROTOCOL = "tcp";
        //context=new
    }

    public static Resources getInstance() {
        if (rs == null) {
            rs = new Resources();
        }
        return rs;
    }

//    public static ZMsg getQueryMessage(byte[] q, byte[] clusterAddr) {
//        ZMsg outgoing = new ZMsg();
//        outgoing.add(clusterAddr);
//        outgoing.add(q);
//        outgoing.add("Q");
//        return outgoing;
//        //outgoing.send(dealer);
//    }
//
//    public static ZMsg getAddMessage(byte[] q) {
//
//        //ZMsg outgoing=ZMsg.newStringMsg("A");
//        ZMsg outgoing = new ZMsg();
//        //outgoing.add(new ZFrame("A"));
//
//        //ZFrame zf = new ZFrame("k");
//        outgoing.add(q);
//        outgoing.add(new ZFrame("A"));
//        return outgoing;
//        //outgoing.send(dealer);
//
//    }
//
//    public static ZMsg getResponseMessage(byte[] resp, byte[] clusterAddr) {
//        ZMsg outgoing = ZMsg.newStringMsg("R");
//        outgoing.push(resp);
//        outgoing.push(clusterAddr);
//        return outgoing;
//        //outgoing.send(dealer);
//
//    }

    public static ZMsg getJoinMessage(byte[] Clusteraddress) {
        ZMsg outgoing = ZMsg.newStringMsg("J");
        outgoing.push(Clusteraddress);
        return outgoing;
        //outgoing.send(dealer);
    }

}
