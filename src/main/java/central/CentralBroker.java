package central;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZSocket;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.awt.peer.SystemTrayPeer;

public class CentralBroker {
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
            Thread.sleep(3000);
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
        //outgoing=ZMsg.newStringMsg("ACK");
        //outgoing.push(incoming.Source);
        //outgoing.send(router);
        switch (m.Type)
        {
            //case ""
        }
    }
    public static void main(String[] args)throws Exception{
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
            Msg incoming = new Msg(ZMsg.recvMsg(router));
            incoming.dump();
            handleMessage(incoming);
        }
    }

}
