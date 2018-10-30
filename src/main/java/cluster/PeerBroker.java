package cluster;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;


/**
 * @author anpham
 * Local cluster or a leaf cluster
 */

public class PeerBroker {
    public ZContext ctx;
    public ZMQ.Socket insertbe;
    public ZMQ.Socket insertfe;
    private String name;


    public PeerBroker(String name) {
        this.name = name;
//        initializeGateway();
    }

    public static void main(String[] args) {
        PeerBroker self = new PeerBroker("Cl1");
        self.initializeGateway();

        while (!Thread.currentThread().isInterrupted()) {
            //  Poll for activity, or 1 second timeout
//            ZMQ.PollItem items[] = {new ZMQ.PollItem(self.insertfe, ZMQ.Poller.POLLIN)};
            ZMQ.Poller poller = self.ctx.createPoller(1);
            poller.register(self.insertfe, ZMQ.Poller.POLLIN);
            int rc = poller.poll(10 * 1000);
            if (rc == -1)
                break; //  Interrupted

            String result = "";

            //  Handle incoming status messages
            if (poller.pollin(0)) {
//            if (items[0].isReadable()) {
                result = new String(self.insertfe.recv(0));
                System.out.println("Receive request from client:\n" + result);
                // do sth and wait for response here
                self.insertfe.send("Insert req RECEIVED: " + result);

                System.out.println("Broadcast request from client:\n" + result);
                self.insertbe.send("Broadcast Insert req received: " + result);
            }
        }
//        self.ctx.destroy();
    }

    public void initializeGateway() {
        ctx = new ZContext();

        // Socket to talk to peer node
        insertfe = ctx.createSocket(ZMQ.REP);
//        insertfe.bind("tcp://*:2261");
        insertfe.bind(String.format("ipc://%s-insertfe.ipc", name));


        //  Bind insert backend to endpoint
        insertbe = ctx.createSocket(ZMQ.PUB);
        insertbe.bind(String.format("ipc://%s-insertbe.ipc", name));

//        insertbe.bind("tcp://*:2262");

        //  Bind search backend to endpoint
//        ZMQ.Socket searchbe = ctx.createSocket(ZMQ.PUB);
//        searchbe.bind(String.format("ipc://%s-searchbe.ipc", name));
    }

    public String getName() {
        return name;
    }
}
