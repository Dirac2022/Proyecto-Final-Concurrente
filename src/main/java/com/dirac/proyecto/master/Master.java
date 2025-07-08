package com.dirac.proyecto.master;

import com.dirac.proyecto.core.Message;
import com.dirac.proyecto.core.Message.MessageType;
import com.dirac.proyecto.core.WorkerInfo;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class Master {
    private final int port;
    private ServerSocket serverSocket;
    private final List<WorkerInfo> workers = new CopyOnWriteArrayList<>(); // Lista segura para hilos
    private final ConcurrentHashMap<Integer, double[]> results = new ConcurrentHashMap<>();
    private CountDownLatch resultLatch;

    public Master(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        serverSocket = new ServerSocket(port);
        System.out.println("Master iniciado en el puerto " + port + ". Esperando workers...");
        
        // Hilo para aceptar nuevas conexiones de workers
        Thread listenerThread = new Thread(this::listenForWorkers);
        listenerThread.start();
        
        // Hilo para monitorear la salud de los workers (detección de caídas)
        Thread monitorThread = new Thread(this::monitorWorkers);
        monitorThread.setDaemon(true);
        monitorThread.start();
    }
    
    private void listenForWorkers() {
        while (true) {
            try {
                Socket workerSocket = serverSocket.accept();
                System.out.println("Nuevo worker conectado desde " + workerSocket.getInetAddress());
                
                // Hilo dedicado para escuchar a este worker específico
                Thread workerHandlerThread = new Thread(() -> handleWorker(workerSocket));
                workerHandlerThread.start();
            } catch (Exception e) {
                System.err.println("Error aceptando conexión de worker: " + e.getMessage());
            }
        }
    }

    private void handleWorker(Socket workerSocket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream());

            Message registerMsg = (Message) in.readObject();
            if (registerMsg.getType() == MessageType.REGISTER) {
                String workerId = workerSocket.getInetAddress().getHostAddress() + ":" + workerSocket.getPort();
                WorkerInfo info = new WorkerInfo(workerId, workerSocket.getInetAddress().getHostAddress(), workerSocket.getPort(), workerSocket, out);
                workers.add(info);
                System.out.println("Worker " + workerId + " registrado. Total de workers: " + workers.size());

                // Escuchar mensajes de este worker (heartbeats, resultados)
                while (true) {
                    Message msg = (Message) in.readObject();
                    if (msg.getType() == MessageType.HEARTBEAT) {
                        info.setLastHeartbeat(System.currentTimeMillis());
                    } else if (msg.getType() == MessageType.RESULT) {
                        Object[] payload = (Object[]) msg.getPayload();
                        int segmentId = (int) payload[0];
                        double[] segmentResult = (double[]) payload[1];
                        results.put(segmentId, segmentResult);
                        resultLatch.countDown(); // Contar que un resultado ha llegado
                        System.out.println("Resultado recibido para el segmento " + segmentId);
                    }
                }
            }
        } catch (Exception e) {
            // Un worker se desconectó
        }
    }
    
    private void monitorWorkers() {
        while (true) {
            try {
                Thread.sleep(10000); // Revisar cada 10 segundos
                long now = System.currentTimeMillis();
                for (WorkerInfo worker : workers) {
                    // Si no hemos recibido un heartbeat en más de 15 segundos, lo consideramos caído
                    if (now - worker.getLastHeartbeat() > 15000) {
                        System.out.println("CRITICAL: Worker " + worker.getId() + " está caído! Eliminando de la lista.");
                        workers.remove(worker);
                        // Aquí iría la lógica de recuperación (Ejemplo 3)
                        // Por ejemplo, encontrar la réplica y promoverla.
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public DArrayDouble createDArrayDouble(double[] fullArray) {
        return new DArrayDouble(fullArray, this);
    }

    // Clase interna para manejar los arrays distribuidos
    public class DArrayDouble {
        private final double[] originalArray;
        private final Master master;
        private int segmentCount;

        DArrayDouble(double[] array, Master master) {
            this.originalArray = array;
            this.master = master;
        }

        public double[] applyOperation(String operation) throws InterruptedException {
            if (master.workers.isEmpty()) {
                System.err.println("No hay workers para procesar. Abortando.");
                return null;
            }

            // 1. Segmentar y distribuir el array
            distribute(originalArray);

            // 2. Enviar la orden de computar
            System.out.println("Enviando orden de computación a todos los workers...");
            master.resultLatch = new CountDownLatch(segmentCount); // Esperar `segmentCount` resultados
            
            for (WorkerInfo worker : master.workers) {
                if (worker.getPrimarySegmentId() != -1) { // Solo enviar a workers con datos primarios
                    Object[] payload = new Object[]{worker.getPrimarySegmentId(), operation};
                    worker.sendMessage(new Message(MessageType.COMPUTE, payload));
                }
            }

            // 3. Esperar a que todos los resultados lleguen
            System.out.println("Esperando resultados de " + segmentCount + " segmentos...");
            master.resultLatch.await(); // El hilo principal se bloquea aquí

            // 4. Consolidar resultados
            return consolidateResults();
        }

        private void distribute(double[] array) {
            this.segmentCount = master.workers.size();
            int chunkSize = (int) Math.ceil((double) array.length / segmentCount);
            
            for (int i = 0; i < segmentCount; i++) {
                WorkerInfo worker = master.workers.get(i);
                int start = i * chunkSize;
                int end = Math.min(start + chunkSize, array.length);
                double[] segment = Arrays.copyOfRange(array, start, end);
                
                worker.setPrimarySegmentId(i);
                Object[] payload = new Object[]{i, segment};
                worker.sendMessage(new Message(MessageType.DATA, payload));
                System.out.println("Enviando segmento " + i + " a worker " + worker.getId());
            }
        }
        
        private double[] consolidateResults() {
            System.out.println("Consolidando resultados...");
            double[] finalArray = new double[originalArray.length];
            int position = 0;
            for (int i = 0; i < segmentCount; i++) {
                double[] segmentResult = master.results.get(i);
                System.arraycopy(segmentResult, 0, finalArray, position, segmentResult.length);
                position += segmentResult.length;
            }
            master.results.clear(); // Limpiar para la próxima operación
            return finalArray;
        }
    }
}