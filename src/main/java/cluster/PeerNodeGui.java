package cluster;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class PeerNodeGui {
    private JPanel mainPanel;
    private JTextArea txtInp;
    private JButton btnInsert;
    private JButton btnSearch;
    private JTextArea txtOut;
    private JTextArea txtHistory;
    private JTextArea txtInfo;

    private PeerNode peerNode;

    public PeerNodeGui(PeerNode node) {
        peerNode = node;
        final PeerNodeGui self = this;

        btnInsert.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendInsert(txtInp.getText());
            }
        });

        btnSearch.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendQuery(txtInp.getText());
                track("Sent query request: " + txtInp.getText() + "\n");
                track("Wating for reply.....\n");
            }
        });
    }

    public void printlnOut(String text) {
        txtOut.append(text + "\n");
    }

    public void track(String text) {
        txtHistory.append(text);
    }

    public void showup() {
        this.txtInfo.append("Node ID: " + peerNode.myID + "\n");

        JFrame frame = new JFrame();
        frame.setContentPane(this.mainPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
