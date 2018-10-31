package central;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZSocket;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.awt.peer.SystemTrayPeer;
import java.util.Hashtable;

public class CentralBroker {
//    Hashtable<String, Integer> numbers
//            = new Hashtable<String, Integer>();

    private static ZContext context;
    private static ZMQ.Socket publisher;
    private static ZMQ.Socket router;

    //Init stuff in constructor......
    public CentralBroker(){
        context=new ZContext(1);
        publisher=context.createSocket(ZMQ.PUB);
        router=context.createSocket(ZMQ.ROUTER);

        //bindings....
        publisher.bind(String.format("%s://localhost:%s",
                Resources.getInstance().PUB_SUB_PROTOCOL,Resources.getInstance().PUB_SUB_PORT));
        router.bind(String.format("%s://localhost:%s",
                Resources.getInstance().ROUTER_DEALER_PROTOCOL,Resources.getInstance().ROUTER_DEALER_PORT));
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.err.println("ROUTER of the central broker was sleeping and waiting for others to join... Please don't interrupt!");
        }
        System.out.println("finished Initing Central Broker");
    }

    public static void cleanup()
    {
        publisher.close();
        router.close();
        context.destroy();
    }

    public static void handleMessage(Msg m){
        ZMsg outgoing;
        //outgoing=ZMsg.newStringMsg("ACK");
        //outgoing.push(incoming.Source);
        //outgoing.send(router);
        System.out.println("handdling message..........");
        switch (m.Type.charAt(0))
        {
            case 'Q':
            case 'A':
                System.out.println("Add or Query message");
                ZMsg msg= Resources.getInstance().getAddMessage(m.Command.getBytes());
                msg.dump();
                msg.send(publisher);
                break;

            case 'R':
                System.out.println("Response message");
                Resources.getInstance().getResponseMessage(m.Command.getBytes(),m.Destination.getBytes());
                break;
            case 'J':
                Resources.getInstance().getJoinMessage(m.Source.getBytes());
                System.out.println("Join message");
                break;
        }//TODO: protocol error here
    }
    public static void main(String[] args){

        //initialize sockets
        new CentralBroker();

        //handle interrupt to cleanup properly
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println("How dare you interrupted me bastard?");
                cleanup();
            }
        }));

        ZMsg outgoing;//= ZMsg.newStringMsg("ACK");

        //mainloop
        while(true)
        {
            Msg incoming = null;
            try {
                incoming = new Msg(ZMsg.recvMsg(router));
            } catch (Exception e) {
                e.printStackTrace();
            }
            incoming.dump();
            handleMessage(incoming);
        }
    }

}
