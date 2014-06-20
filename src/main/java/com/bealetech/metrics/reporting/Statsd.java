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

    public static enum StatType { COUNTER, TIMER, GAUGE }

    private final String host;
    private final int port;

    private boolean prependNewline = false;

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

        prependNewline = false;

        datagramSocket = new DatagramSocket();

        dataCollector = new DataCollector(datagramSocket.getReceiveBufferSize());

        outputData.reset();
    }

    private class DataCollector {
        public List<List<String>> storage = new ArrayList<List<String>>();
        private int currentSize = 0;
        private int storageCapacity = 0;

        public DataCollector(int capacity) {
            storageCapacity = capacity > 0 ? capacity : 1024;
            storage.add(new ArrayList<String>());
        }

        public void add(String newData) {
            if (null == newData || newData.length() == 0) {
                return;
            }
            newData = newData.concat("\n");
            if (currentSize + newData.length() >= storageCapacity) {
                storage.add(new ArrayList<String>());
                currentSize = 0;
            }
            else {
                currentSize = currentSize + newData.length();
            }
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
            dataBuffer = new byte[8192];
        }

        try {
            return new DatagramPacket(dataBuffer, dataBuffer.length, InetAddress.getByName(this.host), this.port);
        } catch (Exception e) {
            return null;
        }
    }
}
