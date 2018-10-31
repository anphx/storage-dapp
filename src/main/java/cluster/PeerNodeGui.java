package cluster;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class PeerNodeGui {
    private JPanel mainPanel;
    private JTextArea txtInput;
    private JButton btnInsert;
    private JButton btnSearch;
    private JTextArea txtResult;
    private JTextArea txtInfo;

    private PeerNode peerNode;

    public PeerNodeGui(PeerNode node) {
        peerNode = node;
        btnInsert.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendInsert(txtInput.getText());
            }
        });
        btnSearch.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendQuery(txtInput.getText());
            }
        });
    }

    public void showup() {
        this.txtInfo.append(new String(peerNode.myID) + "\n");

        JFrame frame = new JFrame();
        frame.setContentPane(this.mainPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
