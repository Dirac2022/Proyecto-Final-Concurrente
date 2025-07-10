package com.dirac.proyecto.core;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class WorkerInfo {
    private final String id;
    private final String address;
    private final int listenPort;
    private final transient ObjectOutputStream out;
    private long lastHeartbeat;
    private int primarySegmentId = -1;

    public WorkerInfo(String id, String address, int listenPort, ObjectOutputStream out) {
        this.id = id; this.address = address; this.listenPort = listenPort; this.out = out; this.lastHeartbeat = System.currentTimeMillis();
    }
    
    public synchronized void sendMessage(Message msg) {
        try {
            out.writeObject(msg);
            out.flush();
        } catch (Exception e) { /* El Master se encargará de la desconexión */ }
    }

    public synchronized void resetStream() {
        try {
            out.reset();
        } catch (IOException e) { /* El Master se encargará */ }
    }

    public String getId() { return id; }
    public String getAddress() { return address; }
    public int getListenPort() { return listenPort; }
    public int getPrimarySegmentId() { return primarySegmentId; }
    public void setPrimarySegmentId(int id) { this.primarySegmentId = id; }
}