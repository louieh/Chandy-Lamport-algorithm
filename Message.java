import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Message implements Serializable, Comparable<Message> {
    private static final long serialVersionUID = 1L;
    private final String type, content;
    private final int from, to;
    private final int clock;
    private final int span;
    private final int[] timestamp_array;
    private HashMap<Integer, String> statusCollection = new HashMap<>();
    private HashMap<Integer, int[]> timestampCollection = new HashMap<>();

    private Message(MessageBuilder mb) {
        this.from = mb.from;
        this.to = mb.to;
        this.type = mb.type;
        this.content = mb.content;
        this.clock = mb.clock;
        this.span = mb.span;
        this.timestamp_array = mb.timestamp_array;
        this.statusCollection = mb.statusCollection;
        this.timestampCollection = mb.timestampCollection;
    }

    public void sendMsg(Message msg, String hostname, int port) {
        Socket randSocket;
        while (true) {
            try {
                randSocket = new Socket(hostname, port);
                break;
            } catch (IOException e) {
                System.out.println("connect refused retry...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
                e.printStackTrace();
            }
        }

        try {
            OutputStream outToServer = randSocket.getOutputStream();
            ObjectOutputStream outStream = new ObjectOutputStream(outToServer);
            System.out.println("<<<<<< send message to <<<<<< " + hostname + ".utdallas.edu");
            outStream.writeObject(msg);
            // outToServer.close();
            // outStream.close();
        } catch (IOException e) {
            System.out.println("send message wrong...");
            e.printStackTrace();
        }
    }

    @Override
    public int compareTo(Message that) {
        return this.getClock() == that.getClock() ? this.getSender().compareTo(that.getSender()) : this.getClock() - that.getClock();
    }

    @Override
    public String toString() {
        return "";
    }

    public Integer getSender() {
        return from;
    }

    public Integer getReceiver() {
        return to;
    }

    public String getType() {
        return type;
    }

    public String getContent() {
        return content;
    }

    public int getClock() {
        return clock;
    }

    public int getSpan() {
        return span;
    }

    public int[] getTimestamp_array() {
        return timestamp_array;
    }

    public HashMap<Integer, String> getStatusCollection(){
        return statusCollection;
    }

    public HashMap<Integer, int[]> getTimestampCollection(){
        return timestampCollection;
    }
    public static class MessageBuilder {
        private String type, content = "";
        private int clock;
        private int from, to;
        private int span;
        private int[] timestamp_array;
        private HashMap<Integer, String> statusCollection = new HashMap<>();
        private HashMap<Integer, int[]> timestampCollection = new HashMap<>();

        public MessageBuilder from(int from) {
            this.from = from;
            return this;
        }

        public MessageBuilder to(int to) {
            this.to = to;
            return this;
        }

        public MessageBuilder type(String type) {
            this.type = type;
            return this;
        }

        public MessageBuilder clock(int clock) {
            this.clock = clock;
            return this;
        }

        public MessageBuilder content(String content) {
            this.content = content;
            return this;
        }

        public MessageBuilder span(int span) {
            this.span = span;
            return this;
        }

        public MessageBuilder timestamp_array(int[] timestamp_array) {
            this.timestamp_array = timestamp_array;
            return this;
        }

        public MessageBuilder statusCollection(HashMap<Integer, String> statusCollection){
            this.statusCollection = statusCollection;
            return this;
        }
        public MessageBuilder timestampCollection(HashMap<Integer, int[]> timestampCollection){
            this.timestampCollection = timestampCollection;
            return this;
        }

        public Message build() {
            return new Message(this);
        }
    }
}