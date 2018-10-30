/**
 * @param maxChunks: max number of chunks each peer node can store, default = 10
 * @param chunkSize: the size of each data chunk = X bits
 * @param hammingT: Hamming distance threshold, configurable
 * storageArr:  an array of nChunks row, each row stores a Hashtable<address, data>
 */

import cluster.PeerBroker;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Random;

public class PeerNode {
    private PeerBroker peerCluster;
    private ZContext ctx;
    private int maxChunks;
    private int chunkBits;
    private int chunkBytes;
    private float hammingT;

    private String[][] storageArr;


    public PeerNode(PeerBroker cluster, int nChunks, int chSize, int thresholdT) {
        peerCluster = cluster;
        maxChunks = nChunks;
        chunkBits = chSize;
        chunkBytes = toByte(chunkBits);
        hammingT = thresholdT;

        initializeStorage();

        // Initialize context to use for the whole life cycle of a node
        ctx = new ZContext();
        subscribeTo(peerCluster.getName());
    }

    public static void main(String[] args) {
        String name = args[0];
        PeerNode self = new PeerNode(new PeerBroker("Cl1"), 10, 16, 3);
        ZMQ.Socket insertfe = self.ctx.createSocket(ZMQ.REQ);
        insertfe.connect(String.format("ipc://%s-insertfe.ipc", "Cl1"));

        ZMQ.Socket insertbe = self.ctx.createSocket(ZMQ.SUB);
        insertbe.subscribe("".getBytes());
        insertbe.connect(String.format("ipc://%s-insertbe.ipc", "Cl1"));

        System.out.println(name + " sent insert REQ to broker----");
        insertfe.send(name + " want to insert sth.....");
        boolean done = false;

        while (!done) {
            //  Poll for activity, or 1 second timeout
            ZMQ.Poller poller = self.ctx.createPoller(2);
            poller.register(insertfe, ZMQ.Poller.POLLIN);
            poller.register(insertbe, ZMQ.Poller.POLLIN);

            int rc = poller.poll(10 * 1000);
            if (rc == -1)
                break; //  Interrupted

            //  Handle incoming status messages
            if (poller.pollin(0)) {
                String result = new String(insertfe.recv(0));
                System.out.println("Server answer:\n" + result);
            } else if (poller.pollin(1)) {
                // Receive broadcast msg
                String result = new String(insertbe.recv(0));
                System.out.println("Server BROADCAST:\n" + result);
            } else {
                done = true;
            }

        }
    }

    private int toByte(int xBits) {
        return Math.round(xBits / 8);
    }

    private void subscribeTo(String clusterName) {
        //  Subscribe to the insertbe of cluster for any inserting req
        ZMQ.Socket insertbe = ctx.createSocket(ZMQ.SUB);
        insertbe.subscribe("".getBytes());
        insertbe.connect("tcp://localhost:2262");
    }

    private int hammingDist(String left, String right) {
        if (left.length() != right.length() || left.getBytes().length != chunkBytes)
            return -1;

        int distance = 0;

        for (int i = 0; i < left.length(); i++) {
            if (left.charAt(i) != right.charAt(i)) {
                distance++;
            }
        }

        return distance;
    }

    private void initializeStorage() {
        storageArr = new String[maxChunks][];
        for (int i = 0; i < storageArr.length; i++) {
            String[] arr = new String[2];
            byte[] rand = new byte[chunkBytes];
            new Random().nextBytes(rand);
            String key = new String(rand);

            arr[0] = key;
            arr[1] = new String(new byte[chunkBytes]);

            storageArr[i] = arr;
        }
    }

    private int insert(String inputStr) {
        // Check size constraint
        if (inputStr.getBytes().length != chunkBytes) return 0;

        // Find suitable memory
        for (int i = 0; i < storageArr.length; i++) {
            int dist = hammingDist(storageArr[i][0], inputStr);
            if (dist <= hammingT) {
                storageArr[i][1] = inputStr;
            }
        }
        // Save
        return 1;
    }

    private int search(String inputStr) {
        // Check size constraint
        if (inputStr.getBytes().length != chunkBytes) return 0;
        return 0;
    }
}
