package com.dirac.proyecto.core;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class WorkerInfo {
    private final String id; // ID del Worker
    private final String address; // Dirección IP del Worker
    private final int listenPort; // Puerto de escucha del Worker
    private final transient ObjectOutputStream out; // Flujo de salida para enviar mensajes al Worker
    private long lastHeartbeat; // Marca de tiempo del último latido (heartbeat) del Worker
    // primarySegmentId es el ID del segmento primario que este Worker está procesando.
    // Tiene -1 al inicializar, indicando que aún no se ha asignado un segmento primario.
    private int primarySegmentId = -1;
    // replicaSegmentIds es una lista de IDs de segmentos que este Worker está replicando.
    // esto significa que este Worker es un secundario para esos segmentos.
    private final List<Integer> replicaSegmentIds = new ArrayList<>();

    public WorkerInfo(String id, String address, int listenPort, ObjectOutputStream out) {
        this.id = id;
        this.address = address;
        this.listenPort = listenPort;
        this.out = out;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    // Getters y Setters
    public String getId() { return id; }
    public String getAddress() { return address; }
    public int getListenPort() { return listenPort; }
    public int getPrimarySegmentId() { return primarySegmentId; }
    public void setPrimarySegmentId(int id) { this.primarySegmentId = id; }
    public void addReplicaSegmentId(int id) { this.replicaSegmentIds.add(id); }
    public void setLastHeartbeat() { this.lastHeartbeat = System.currentTimeMillis(); }

    // Este metodo envía un mensaje al Worker a través del flujo de salida.
    // Si ocurre un error al enviar el mensaje, se asume que el Worker está desconectado
    // y el Master se encargará de manejar la desconexión.
    // Se usa synchronized para evitar problemas de concurrencia al enviar mensajes.
    // El mensaje puede ser de tipo Message, que contiene el tipo de mensaje y el payload.
    // El payload puede ser cualquier objeto que se quiera enviar al Worker.
    public synchronized void sendMessage(Message msg) {
        try {
            out.writeObject(msg);
            out.flush();
        } catch (IOException e) {
            // El Master manejará la desconexión
            System.out.println("Error al enviar mensaje al Worker " + id + ": " + e.getMessage());
        }
    }

    public synchronized void resetStream() {
        try {
            out.reset();
        } catch (IOException e) { /* El Master ya manejará la desconexión si ocurre */ }
    }

}