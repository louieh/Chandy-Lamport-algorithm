import java.lang.Thread;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class NodeListener extends Thread {
    Node node;
    int port;
    private ServerSocket serverSocket;

    public NodeListener(Node node, int port) throws IOException {
        this.node = node;
        this.port = port;
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (true) {
            try {
//                if (this.node.ifMAPStop) {
//                    System.out.println("Node " + this.node.nodeID + " listener thread stop...");
//                    return;
//                }
                // System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
                Socket server = serverSocket.accept();

                // get input
                // System.out.println("Just connected to " + server.getRemoteSocketAddress());
                ObjectInputStream in = new ObjectInputStream(server.getInputStream());
                Message msg = (Message) in.readObject();
                this.node.receiveMsg(msg);

                // output to other node
//                ObjectOutputStream out = new ObjectOutputStream(server.getOutputStream());
//                out.writeUTF("Thank you for connecting to " + server.getLocalSocketAddress() + "\nGoodbye!");
//                server.close();
            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}