package central;

import common.Msg;
import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class CentralBroker {
    private static ZContext context;
    private static ZMQ.Socket publisher;
    private static ZMQ.Socket router;

    //Init stuff in constructor......
    public CentralBroker() {
        context = new ZContext(1);
        publisher = context.createSocket(ZMQ.PUB);
        router = context.createSocket(ZMQ.ROUTER);

        //bindings....
        publisher.bind(String.format(Shared.CENTRAL_LOCAL_ADDR, Shared.PUB_SUB_PROTOCOL, Shared.PUB_SUB_PORT));
//        publisher.bind(String.format("%s://localhost:%s",
//                Resources.getInstance().PUB_SUB_PROTOCOL, Resources.getInstance().PUB_SUB_PORT));
        router.bind(String.format(Shared.CENTRAL_LOCAL_ADDR, Shared.ROUTER_DEALER_PROTOCOL, Shared.ROUTER_DEALER_PORT));
// router.bind(String.format("%s://localhost:%s",
//                Resources.getInstance().ROUTER_DEALER_PROTOCOL, Resources.getInstance().ROUTER_DEALER_PORT));
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.err.println("ROUTER of the central broker was sleeping and waiting for others to join... Please don't interrupt!");
        }
        System.out.println("finished initializing Central Broker");
    }

    public static void cleanup() {
        publisher.close();
        router.close();
        context.destroy();
    }

    public static void handleMessage(Msg m) {
        ZMsg outgoing;
        //outgoing=ZMsg.newStringMsg("ACK");
        //outgoing.push(incoming.Source);
        //outgoing.send(router);
        //System.out.println("handling message..........");
        //m.dump();
        ZMsg msg;
        try {

            switch (m.Type.charAt(0)) {
                case 'Q':
                    System.out.println("Query message");
                    msg = Shared.getQueryMessage(m.Command, m.Destination.getBytes());
                    //msg.dump();
                    msg.send(publisher);
                    break;
                case 'A':
                    System.out.println("Add message");
                    msg = Shared.getAddMessage(m.Command, publisher);
                    //msg.dump();
                    msg.send(publisher);
                    break;
                case 'R':
                    System.out.println("Response message");
//                    Shared.getResponseMessage(m.Command, m.Destination.getBytes()).send(router);
                    // AnP: Source is the sender broker in this case
                    router.sendMore(m.Source);
                    router.send(m.Command);
                    break;
                //            case 'J':
                //                Resources.getInstance().getJoinMessage(m.Source.getBytes());
                //                System.out.println("Join message");
                //                break;
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

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
        Msg incoming = null;
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(router, ZMQ.Poller.POLLIN);

        while (true) {
            int rc = poller.poll(1000);
            //System.out.println("somewhere in main looop");
            if (rc == -1)
                break; //  Interrupted
            if (poller.pollin(0)) {
                try {
                    ZMsg msg = ZMsg.recvMsg(router);
                    //msg.dump();
                    incoming = new Msg(msg);
//                    incoming.dump();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleMessage(incoming);
            }
        }
    }

}
