package com.dirac.proyecto.worker;

import com.dirac.proyecto.core.Message;
import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;

public class Worker {
    private final String masterAddress;
    private final int masterPort;
    private final int listenPort;
    private ObjectOutputStream masterOut;
    private String workerId;
    private final Object masterOutputLock = new Object();
    private final Map<Integer, Object> primarySegments = new ConcurrentHashMap<>();
    private final Map<Integer, Object> replicaSegments = new ConcurrentHashMap<>();

    public Worker(String masterAddress, int masterPort, int listenPort) {
        this.masterAddress = masterAddress; this.masterPort = masterPort; this.listenPort = listenPort; this.workerId = "Worker@" + listenPort;
    }

    public void start() throws Exception {
        new Thread(this::startReplicaServer).start();
        Socket masterSocket = new Socket(masterAddress, masterPort);
        masterOut = new ObjectOutputStream(masterSocket.getOutputStream());
        ObjectInputStream masterIn = new ObjectInputStream(masterSocket.getInputStream());
        synchronized (masterOutputLock) {
            masterOut.writeObject(new Message(Message.MessageType.REGISTER, listenPort));
            masterOut.flush();
        }
        startHeartbeat(masterSocket);
        listenForMasterCommands(masterIn);
    }
    
    private void startReplicaServer() {
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            while (true) {
                Socket sourceWorkerSocket = serverSocket.accept();
                new Thread(() -> handleReplicaData(sourceWorkerSocket)).start();
            }
        } catch (IOException e) { System.exit(1); }
    }

    private void handleReplicaData(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Message msg = (Message) in.readObject();
            if (msg.getType() == Message.MessageType.REPLICA_DATA) {
                Object[] payload = (Object[]) msg.getPayload();
                replicaSegments.put((Integer) payload[0], payload[1]);
            }
        } catch (Exception e) { /* Ignorar */ }
    }

    private void listenForMasterCommands(ObjectInputStream in) {
        try {
            while (true) {
                Message msg = (Message) in.readObject();
                 switch (msg.getType()) {
                    case DATA:
                        Object[] payload = (Object[]) msg.getPayload();
                        primarySegments.put((Integer) payload[0], payload[1]);
                        System.out.println("[" + workerId + "-INFO] Segmento primario " + payload[0] + " recibido/actualizado.");
                        break;
                    case REPLICATE_ORDER:
                        handleReplicateOrder((Object[]) msg.getPayload());
                        break;
                    case COMPUTE:
                        performComputation((Object[]) msg.getPayload());
                        break;
                    default:
                }
            }
        } catch (Exception e) { System.exit(1); }
    }
    
    private void handleReplicateOrder(Object[] payload) {
        new Thread(() -> {
            try {
                int segmentId = (int) payload[0]; String replicaAddress = (String) payload[1]; int replicaPort = (int) payload[2];
                Object dataToReplicate = primarySegments.get(segmentId);
                if (dataToReplicate == null) return;
                try (Socket replicaSocket = new Socket(replicaAddress, replicaPort);
                     ObjectOutputStream replicaOut = new ObjectOutputStream(replicaSocket.getOutputStream())) {
                    replicaOut.writeObject(new Message(Message.MessageType.REPLICA_DATA, new Object[]{segmentId, dataToReplicate}));
                }
            } catch (Exception e) { /* Ignorar */ }
        }).start();
    }

    @SuppressWarnings("unchecked")
    private <T extends Number> void performComputation(Object[] payload) throws Exception {
        int segmentId = (int) payload[0]; String operation = (String) payload[1];
        Object dataObject = primarySegments.get(segmentId);
        if (dataObject == null) return;

        T[] data = (T[]) dataObject;
        Number[] results = new Number[data.length];
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        
        for (int i = 0; i < data.length; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    T x = data[index];
                    if ("math".equals(operation)) {
                        results[index] = Math.pow(Math.sin(x.doubleValue()) + Math.cos(x.doubleValue()), 2) / (Math.sqrt(Math.abs(x.doubleValue())) + 1);
                    } else if ("conditional".equals(operation)) {
                        int val = x.intValue();
                        if ((val % 3 == 0) || (val >= 500 && val <= 1000)) {
                            if (val == 0) throw new ArithmeticException("log(0)");
                            results[index] = (int) ((val * Math.log(val)) % 7);
                            // System.out.println("Entra al calculo condicional: " + val + " -> " + results[index]);
                        } else {
                            results[index] = val;
                        }
                    }
                } catch (Exception e) { results[index] = -1; }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("[" + workerId + "-DEBUG] Array de resultados A PUNTO DE SER ENVIADO: " + Arrays.toString(results));
        synchronized(masterOutputLock) {
            masterOut.writeObject(new Message(Message.MessageType.RESULT, new Object[]{segmentId, results}));
            masterOut.flush();
            masterOut.reset(); // Limpiar el stream para evitar problemas de serialización
        }
    }
    
    private void startHeartbeat(Socket masterSocket) {
        Thread t = new Thread(() -> {
            while (!masterSocket.isClosed()) {
                try {
                    Thread.sleep(5000);
                    synchronized(masterOutputLock) {
                        masterOut.writeObject(new Message(Message.MessageType.HEARTBEAT, null));
                        masterOut.flush();
                        masterOut.reset(); // Limpiar el stream para evitar problemas de serialización
                    }
                } catch (Exception e) { break; }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) {
        if (args.length < 3) { System.out.println("Uso: java com.dirac.proyecto.worker.Worker <master_ip> <master_port> <my_listen_port>"); return; }
        try { new Worker(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2])).start(); } catch (Exception e) { e.printStackTrace(); }
    }
}