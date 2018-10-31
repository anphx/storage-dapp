 /**
 * @author: anpham
 * This class handle storing and searching mechanism inside a peer node
 * All requests will be directed to peer Broker for handling.
 *
 * @param maxChunks: max number of chunks each peer node can store, default = 10
 * @param chunkSize: the size of each data chunk = X bits
 * @param hammingT: Hamming distance threshold, configurable
 * @param sumThreshold: Threshold C of position-wise summation.
 *
 * storageArr:  an array of nChunks row, each row stores a Hashtable<address, data>
 */

package cluster;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import smile.math.distance.HammingDistance;

import java.util.BitSet;
import java.util.Random;

public class PeerNode implements Runnable {
    private PeerBroker peerCluster;
    private ZContext ctx;
    private int maxChunks;
    private int chunkBits;
    private int chunkBytes;
    private float hammingT;
    private Boolean isGui;
    private PeerNodeGui myGui;

    private ZMQ.Socket insertbe;

    private Object[][] storageArr;


    public PeerNode(PeerBroker cluster, int nChunks, int chSize, int thresholdT, boolean withGui) {
        peerCluster = cluster;
        maxChunks = nChunks;
        chunkBits = chSize;
        chunkBytes = toByte(chunkBits);
        hammingT = thresholdT;
        isGui = withGui;

        initializeStorage();

        // Initialize context to use for the whole life cycle of a node
        ctx = new ZContext();
        subscribeTo(peerCluster.getName());
    }

    public void run() {
        if (isGui) {
            myGui = new PeerNodeGui(this);
            myGui.showup();
        }

        boolean done = false;

        while (!done) {
            //  Poll for activity, or 1 second timeout
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(insertbe, ZMQ.Poller.POLLIN);

            // Wait 10ms for a response, otherwise dismiss
            int rc = poller.poll(10 * 1000);
            if (rc == -1)
                break; //  Interrupted

            //  Handle incoming requests
            if (poller.pollin(1)) {
                // Receive broadcast msg
                String result = new String(insertbe.recv(0));
                System.out.println("Server BROADCAST:\n" + result);
            } else {
                done = true;
            }

        }
    }

    public static void main(String[] args) {
        //TODO: change for deployment
        String name = args[0];
        PeerNode self = new PeerNode(new PeerBroker("Cl1"), 10, 16, 3, false);
        ZMQ.Socket insertfe = self.ctx.createSocket(ZMQ.REQ);
        insertfe.connect(String.format("ipc://%s-insertfe.ipc", "Cl1"));

        ZMQ.Socket insertbe = self.ctx.createSocket(ZMQ.SUB);
        insertbe.subscribe("".getBytes());
        insertbe.connect(String.format("ipc://%s-insertbe.ipc", "Cl1"));

        System.out.println(name + " sent insert REQ to broker----");
        insertfe.send(name + " want to insert sth.....");
        boolean done = false;

//        while (!done) {
//            //  Poll for activity, or 1 second timeout
//            ZMQ.Poller poller = self.ctx.createPoller(2);
//            poller.register(insertfe, ZMQ.Poller.POLLIN);
//            poller.register(insertbe, ZMQ.Poller.POLLIN);
//
//            int rc = poller.poll(10 * 1000);
//            if (rc == -1)
//                break; //  Interrupted
//
//            //  Handle incoming status messages
//            if (poller.pollin(0)) {
//                String result = new String(insertfe.recv(0));
//                System.out.println("Server answer:\n" + result);
//            } else if (poller.pollin(1)) {
//                // Receive broadcast msg
//                String result = new String(insertbe.recv(0));
//                System.out.println("Server BROADCAST:\n" + result);
//            } else {
//                done = true;
//            }
//
//        }
    }

    private int toByte(int xBits) {
        return Math.round(xBits / 8);
    }

    private void subscribeTo(String clusterName) {
        //  Subscribe to the insertbe of cluster for any inserting/searching req
        insertbe = ctx.createSocket(ZMQ.SUB);
        insertbe.subscribe("".getBytes());
        insertbe.connect(String.format(Shared.PUBLISH_SOCK_ADDR, clusterName));
    }

    private void initializeStorage() {
        storageArr = new Object[maxChunks][];
        for (int i = 0; i < maxChunks; i++) {
            Object[] arr = new Object[2];

            arr[0] = randomize();
            arr[1] = new int[chunkBits];
            storageArr[i] = arr;
        }
    }

    private int[] sumOf(int[] curr, BitSet input) {
        if (input.size() != chunkBits) {
            System.out.println("Invalid format of input, should be " + chunkBits + " bits in size");
            return new int[chunkBits];
        }
        for (int i = 0; i < chunkBits; i++) {
            int val = input.get(i) ? 1 : -1;
            curr[i] += val;
        }
        return curr;
    }

    private BitSet randomize() {
        Random random = new Random();
        BitSet bits = new BitSet(chunkBits);
        for (int i = 0; i < chunkBits; ++i)
            bits.set(i, random.nextBoolean());

        return bits;
    }

    private int doInsert(BitSet inputStr) {
        // Check size constraint
        if (inputStr.size() != chunkBits) return 0;

        // Find suitable memory
        for (int i = 0; i < storageArr.length; i++) {
            int addrDist = HammingDistance.d((BitSet) storageArr[i][0], inputStr);
            if (addrDist <= hammingT) {
                storageArr[i][1] = sumOf((int[]) storageArr[i][1], inputStr);
            }
        }
        return 1;
    }

    private int[] doMatch(BitSet inputStr) {
        // Check size constraint
        if (inputStr.size() != chunkBits) return null;
        int[] resultArr = new int[chunkBits];
        for (int i = 0; i < storageArr.length; i++) {
            int addrDist = HammingDistance.d((BitSet) storageArr[i][0], inputStr);
            if (addrDist <= hammingT) {
                resultArr = sumOf(resultArr, inputStr);
            }
        }
        return resultArr;
    }

}
