package com.bealetech.metrics.reporting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A client to a StatsD server.
 */
public class Statsd implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Statsd.class);

    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
    private static final int MAX_CAPACITY = 1024 * 8;
    private static final int MIN_CAPACITY = 1024;

    public static enum StatType { COUNTER, TIMER, GAUGE }

    private final String host;
    private final int port;

    private ByteArrayOutputStream outputData;
    private DatagramSocket datagramSocket;
    private DataCollector dataCollector;

    public Statsd(String host, int port) {
        this.host = host;
        this.port = port;

        outputData = new ByteArrayOutputStream();
    }

    public void connect() throws IllegalStateException, SocketException {
        if(datagramSocket != null) {
            throw new IllegalStateException("Already connected");
        }

        datagramSocket = new DatagramSocket();

        dataCollector = new DataCollector(datagramSocket.getReceiveBufferSize());

        outputData.reset();
    }


    /**
     * A structure for storing data to send,
     * it consists of lists of String.
     * The size of each list is less than given _capacity_
     * in order do not overflow transport buffer (DatagramSocket-> ReceiveBufferSize)
     */
    private class DataCollector {
        private List<List<String>> storage = new ArrayList<List<String>>();
        private int currentSize = 0;
        private int storageCapacity = 0;

        public DataCollector(int capacity) {
            if (capacity <= 0 || capacity > MAX_CAPACITY) {
                storageCapacity = MIN_CAPACITY;
            }
            storage.add(new ArrayList<String>());
        }


        // Adds a new piece of data to the last sublist of storage.
        // If the size of current sublist is going to be bigger than
        // _storageCapacity_ then it starts a new sublist.
        public void add(String newData) {
            if (null == newData || newData.length() == 0) {
                return;
            }
            newData = newData.concat("\n");
            if (currentSize + newData.length() >= storageCapacity) {
                storage.add(new ArrayList<String>());
                currentSize = 0;
            }
            currentSize = currentSize + newData.length();
            storage.get(storage.size() - 1).add(newData);
        }
    }

    public void send(String name, String value, StatType statType) throws IOException {
        String statTypeStr = "";
        switch (statType) {
            case COUNTER:
                statTypeStr = "c";
                break;
            case GAUGE:
                statTypeStr = "g";
                break;
            case TIMER:
                statTypeStr = "ms";
                break;
        }

        String data = sanitizeString(name).concat(":").concat(value).concat("|").concat(statTypeStr);
        dataCollector.add(data);
    }


    @Override
    public void close() throws IOException {
        for (List<String> list : dataCollector.storage) {
            ByteArrayOutputStream outputDataBuffer = new ByteArrayOutputStream();
            for (String line : list) {
                outputDataBuffer.write(line.getBytes());
            }
            DatagramPacket packet = newPacket(outputDataBuffer);
            packet.setData(outputDataBuffer.toByteArray());
            datagramSocket.send(packet);
        }

        if(datagramSocket != null) {
            datagramSocket.close();
        }
        this.datagramSocket = null;
        dataCollector.storage.clear();
    }


    private String sanitizeString(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }

    private DatagramPacket newPacket(ByteArrayOutputStream out) {
        byte[] dataBuffer;

        if (out != null) {
            dataBuffer = out.toByteArray();
        }
        else {
            dataBuffer = new byte[MAX_CAPACITY];
        }

        try {
            return new DatagramPacket(dataBuffer, dataBuffer.length, InetAddress.getByName(this.host), this.port);
        } catch (Exception e) {
            return null;
        }
    }
}
