package com.dirac.proyecto.core;

import java.io.ObjectOutputStream;
import java.net.Socket;

public class WorkerInfo {
    private final String id;
    private final String address;
    private final int port;
    private transient Socket socket; // 'transient' para que no se intente serializar
    private transient ObjectOutputStream out;
    private long lastHeartbeat;
    private int primarySegmentId = -1; // -1 si no tiene segmento primario

    public WorkerInfo(String id, String address, int port, Socket socket, ObjectOutputStream out) {
        this.id = id;
        this.address = address;
        this.port = port;
        this.socket = socket;
        this.out = out;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    // Getters y Setters
    public String getId() { return id; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public ObjectOutputStream getOut() { return out; }
    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
    public int getPrimarySegmentId() { return primarySegmentId; }
    public void setPrimarySegmentId(int segmentId) { this.primarySegmentId = segmentId; }

    // Método para enviar un mensaje a este worker específico
    public synchronized void sendMessage(Message msg) {
        try {
            out.writeObject(msg);
            out.flush();
        } catch (Exception e) {
            System.err.println("Error enviando mensaje al worker " + id + ": " + e.getMessage());
        }
    }
}