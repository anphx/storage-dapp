package cluster;

import javax.swing.*;

public class PeerNodeGui {
    private JPanel mainPanel;

    private PeerNode peerNode;

    public PeerNodeGui(PeerNode node) {
        peerNode = node;
    }

    public void showup() {
        JFrame frame = new JFrame();
        frame.setContentPane(this.mainPanel);
        frame.pack();
        frame.setVisible(true);

    }
}
