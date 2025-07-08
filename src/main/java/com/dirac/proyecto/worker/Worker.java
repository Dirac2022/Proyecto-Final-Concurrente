package com.dirac.proyecto.worker;

import com.dirac.proyecto.core.Message;
import com.dirac.proyecto.core.Message.MessageType;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Worker {
    private final String masterAddress;
    private final int masterPort;
    private Socket masterSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    // Almacena los segmentos de datos. La clave es el ID del segmento.
    private final Map<Integer, double[]> dataSegments = new HashMap<>();

    public Worker(String masterAddress, int masterPort) {
        this.masterAddress = masterAddress;
        this.masterPort = masterPort;
    }

    public void start() throws Exception {
        System.out.println("Worker iniciando, conectando a Master en " + masterAddress + ":" + masterPort);
        masterSocket = new Socket(masterAddress, masterPort);
        out = new ObjectOutputStream(masterSocket.getOutputStream());
        in = new ObjectInputStream(masterSocket.getInputStream());

        // 1. Registrarse con el Master
        out.writeObject(new Message(MessageType.REGISTER, null));
        out.flush();

        // 2. Iniciar el hilo de Heartbeat
        startHeartbeat();

        // 3. Escuchar comandos del Master en un bucle infinito
        listenForCommands();
    }

    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (!masterSocket.isClosed()) {
                try {
                    Thread.sleep(5000); // Enviar un heartbeat cada 5 segundos
                    out.writeObject(new Message(MessageType.HEARTBEAT, null));
                    out.flush();
                } catch (Exception e) {
                    System.err.println("No se pudo enviar el heartbeat. Master caído?");
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true); // El hilo no impedirá que el programa termine
        heartbeatThread.start();
    }

    private void listenForCommands() {
        try {
            while (true) {
                Message msg = (Message) in.readObject();
                System.out.println("Comando recibido del Master: " + msg.getType());

                switch (msg.getType()) {
                    case DATA:
                        Object[] dataPayload = (Object[]) msg.getPayload();
                        int segmentId = (int) dataPayload[0];
                        double[] segmentData = (double[]) dataPayload[1];
                        dataSegments.put(segmentId, segmentData);
                        System.out.println("Segmento " + segmentId + " recibido con " + segmentData.length + " elementos.");
                        break;
                    case COMPUTE:
                        // Aquí se realiza el cálculo en paralelo
                        performComputation((Object[]) msg.getPayload());
                        break;
                    // Aquí irían los casos para REPLICATE y PROMOTE_REPLICA
                    default:
                        System.out.println("Comando desconocido: " + msg.getType());
                }
            }
        } catch (Exception e) {
            System.err.println("Conexión con el Master perdida. Terminando.");
            // e.printStackTrace();
        } finally {
            try {
                masterSocket.close();
            } catch (Exception e) { /* ignorar */ }
        }
    }

    private void performComputation(Object[] payload) throws Exception {
        int segmentId = (int) payload[0];
        String operation = (String) payload[1];
        
        double[] data = dataSegments.get(segmentId);
        if (data == null) {
            System.err.println("Error: No se encontraron datos para el segmento " + segmentId);
            return;
        }

        double[] results = new double[data.length];
        
        // Usar todos los núcleos de la CPU
        int cores = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores);

        System.out.println("Iniciando computación en " + cores + " hilos para el segmento " + segmentId);

        // Dividir el trabajo entre los hilos
        int chunkSize = (int) Math.ceil((double) data.length / cores);
        for (int i = 0; i < cores; i++) {
            final int start = i * chunkSize;
            final int end = Math.min(start + chunkSize, data.length);

            executor.submit(() -> {
                for (int j = start; j < end; j++) {
                    double x = data[j];
                    // Ejemplo 1: Operación matemática compleja
                    if (operation.equals("math")) {
                        results[j] = Math.pow(Math.sin(x) + Math.cos(x), 2) / (Math.sqrt(Math.abs(x)) + 1);
                    }
                    // Aquí se añadiría la lógica para el Ejemplo 2 (condicional)
                }
            });
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Esperar a que todos los hilos terminen
        }
        
        System.out.println("Computación terminada para el segmento " + segmentId);

        // Enviar resultados al Master
        Object[] resultPayload = new Object[]{segmentId, results};
        out.writeObject(new Message(MessageType.RESULT, resultPayload));
        out.flush();
    }

    public static void main(String[] args) {
        // Para ejecutar: java Worker.java <master_ip> <master_port>
        if (args.length < 2) {
            System.out.println("Uso: java com.mi_proyecto_distribuido.worker.Worker <master_ip> <master_port>");
            return;
        }
        String masterAddress = args[0];
        int masterPort = Integer.parseInt(args[1]);
        
        try {
            new Worker(masterAddress, masterPort).start();
        } catch (Exception e) {
            System.err.println("Error al iniciar el Worker: " + e.getMessage());
            e.printStackTrace();
        }
    }
}