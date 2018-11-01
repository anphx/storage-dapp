package common;

import org.zeromq.ZMsg;

public class Msg {
    public String Source;
    public byte[] Command;
    public String Type;
    public String Destination;

    public Msg(ZMsg inp) throws Exception {
        int size = inp.size();
        if (size == 3) {
            this.Source = inp.popString();
            this.Command = inp.pop().getData();
            this.Type = inp.popString();
        } else if (size == 4) {
            this.Source = inp.popString();
            this.Destination = inp.popString();
            this.Command = inp.pop().getData();
            this.Type = inp.popString();
        } else if (size == 5) {
            this.Source = inp.popString();
            inp.popString();
            this.Destination = inp.popString();
            this.Command = inp.pop().getData();
            this.Type = inp.popString();
        } else {
            inp.dump();
            //throw new Exception("unsupported message type: Size mismatch...");
        }
    }


    public void dump() {
        System.out.println("---------Dumping Msg Content---------");
        System.out.println("Msg Sender: " + Source);
        System.out.println("Msg Destination: " + Destination);
        System.out.println("Msg Command: " + Command);
        System.out.println("Msg Type: " + Type);
    }
}
