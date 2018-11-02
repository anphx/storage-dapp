package cluster;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.BitSet;

public class PeerNodeGui {
    private JPanel mainPanel;
    private JTextArea txtInp;
    private JButton btnInsert;
    private JButton btnSearch;
    private JTextArea txtOut;
    private JTextArea txtHistory;
    private JTextArea txtInfo;
    private JTextArea txtBitStr;
    private JButton clearConsoleButton;
    private JButton btnConvert;

    private PeerNode peerNode;

    public PeerNodeGui(PeerNode node) {
        peerNode = node;
        final PeerNodeGui self = this;

        btnInsert.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendInsert(txtInp.getText());
                track("=========> Insert request sent: " + txtInp.getText());
            }
        });

        btnSearch.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                peerNode.sendQuery(txtInp.getText());
                track("Sent query request: " + txtInp.getText() + "\n");
                track("Wating for reply.....\n");
            }
        });
        clearConsoleButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                txtOut.setText("");
            }
        });
        btnConvert.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String bitStr = "";
                String input = txtInp.getText();
                BitSet bs = BitSet.valueOf(input.getBytes());

                for (int i = 0; i < bs.length(); i++) {
                    bitStr += bs.get(i) ? "1" : "0";
                }
                txtBitStr.setText(bitStr);
            }
        });
    }

    public void printlnOut(String text) {
        txtOut.append(text + "\n");
    }

    public void track(String text) {
        txtHistory.append(text + "\n");
    }

//    public void printToStorage(String text, boolean newline) {
//        if (newline) {
//            txtBitStr.append(text + "\n");
//        } else {
//            txtBitStr.append(text);
//        }
//    }

    public void printInfo(String text) {
        txtInfo.append(text + "\n");
    }

    public void showup() {
        printInfo("Node ID: " + peerNode.getID() + "\n");

        JFrame frame = new JFrame();
        frame.setContentPane(this.mainPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
