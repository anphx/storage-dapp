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
        ZMsg msg;
        try {

            switch (m.Type.charAt(0)) {
                case 'Q':
                    System.out.println("Query message");
                    msg = Shared.getQueryMessage(m.Command, m.Destination.getBytes());
                    msg.send(publisher);
                    break;
                case 'A':
                    System.out.println("Add message");
                    msg = Shared.getAddMessage(m.Command, publisher);
                    msg.send(publisher);
                    break;
                case 'R':
                    System.out.println("Response message");
                    // AnP: Source is the sender broker in this case
                    router.sendMore(m.Source);
                    router.send(m.Command);
                    break;
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

        ZMsg outgoing;

        //mainloop
        Msg incoming = null;
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(router, ZMQ.Poller.POLLIN);

        while (true) {
            int rc = poller.poll(1000);
            if (rc == -1)
                break; //  Interrupted
            if (poller.pollin(0)) {
                try {
                    ZMsg msg = ZMsg.recvMsg(router);
                    incoming = new Msg(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleMessage(incoming);
            }
        }
    }

}
