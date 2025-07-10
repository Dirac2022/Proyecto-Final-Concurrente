package com.dirac.proyecto.master;

import com.dirac.proyecto.core.Message;
import com.dirac.proyecto.core.WorkerInfo;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Master {
    private final int port;
    private final CopyOnWriteArrayList<WorkerInfo> workers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<Integer, Object> results = new ConcurrentHashMap<>();
    private volatile CountDownLatch resultLatch;
    private final Map<Integer, Object> distributedSegments = new ConcurrentHashMap<>();
    private final Map<Integer, String> replicaLocations = new ConcurrentHashMap<>();
    private String lastOperation;
    private final ExecutorService executionPool = Executors.newCachedThreadPool();
    private final Object masterLock = new Object();

    private static class SendCommand {
        final WorkerInfo target; final Message message; final boolean shouldReset;
        SendCommand(WorkerInfo target, Message message, boolean shouldReset) { 
            this.target = target; 
            this.message = message; 
            this.shouldReset = shouldReset;
        }
    }

    public Master(int port) { this.port = port; }

    public void start() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[Master-INFO] Master iniciado en puerto " + port);
            while (true) {
                Socket workerSocket = serverSocket.accept();
                new Thread(() -> handleWorker(workerSocket)).start();
            }
        }
    }

    public <T extends Number> Future<T[]> submitOperation(T[] array, String operation, Map<Integer, int[]> manualSegments) {
        Callable<T[]> task = () -> {
            List<SendCommand> commandPlan = planOperation(array, operation, manualSegments);
            if (commandPlan == null) return null;
            if (commandPlan.isEmpty()) return array;
            
            for (SendCommand cmd : commandPlan) { 
                cmd.target.sendMessage(cmd.message); 
                if (cmd.shouldReset) {
                    cmd.target.resetStream();
                }
            }
            
            resultLatch.await();
            return consolidateResults(array);
        };
        return executionPool.submit(task);
    }
    
    private <T extends Number> List<SendCommand> planOperation(T[] array, String operation, Map<Integer, int[]> manualSegments) {
        synchronized (masterLock) {
            if (workers.size() < 2) { System.err.println("Error: Se necesitan al menos 2 workers."); return null; }
            this.lastOperation = operation;
            results.clear(); distributedSegments.clear(); replicaLocations.clear();
            workers.forEach(w -> w.setPrimarySegmentId(-1));

            List<WorkerInfo> currentWorkers = new ArrayList<>(workers);
            List<SendCommand> commands = new ArrayList<>();
            distributeAndPlan(array, currentWorkers, commands);
            replicateAndPlan(currentWorkers, commands);
            int segmentCount = distributedSegments.size();
            if (segmentCount > 0) {
                resultLatch = new CountDownLatch(segmentCount);
                for (WorkerInfo worker : currentWorkers) {
                    if (worker.getPrimarySegmentId() != -1) {
                        commands.add(new SendCommand(worker, new Message(Message.MessageType.COMPUTE, new Object[]{worker.getPrimarySegmentId(), operation}), false));
                    }
                }
            }
            return commands;
        }
    }
    
    private <T extends Number> void distributeAndPlan(T[] array, List<WorkerInfo> currentWorkers, List<SendCommand> commands) {
        int numWorkers = currentWorkers.size();
        int chunkSize = (int) Math.ceil((double) array.length / numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            WorkerInfo worker = currentWorkers.get(i); int start = i * chunkSize; int end = Math.min(start + chunkSize, array.length);
            if (start >= end) continue;
            T[] segment = Arrays.copyOfRange(array, start, end);
            distributedSegments.put(i, segment);
            worker.setPrimarySegmentId(i);
            commands.add(new SendCommand(worker, new Message(Message.MessageType.DATA, new Object[]{i, segment}), true));
        }
    }
    
    private void replicateAndPlan(List<WorkerInfo> currentWorkers, List<SendCommand> commands) {
        int numWorkers = currentWorkers.size();
        if (numWorkers < 2) return;
        for (int i = 0; i < numWorkers; i++) {
            WorkerInfo primaryWorker = currentWorkers.get(i);
            if (primaryWorker.getPrimarySegmentId() != -1) {
                WorkerInfo replicaWorker = currentWorkers.get((i + 1) % numWorkers);
                int segmentId = primaryWorker.getPrimarySegmentId();
                replicaLocations.put(segmentId, replicaWorker.getId());
                commands.add(new SendCommand(primaryWorker, new Message(Message.MessageType.REPLICATE_ORDER, new Object[]{segmentId, replicaWorker.getAddress(), replicaWorker.getListenPort()}), false));
            }
        }
    }

    private void handleWorker(Socket workerSocket) {
        WorkerInfo info = null;
        String workerId = workerSocket.getInetAddress().getHostAddress() + ":" + workerSocket.getPort();
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {
            Message registerMsg = (Message) in.readObject();
            info = new WorkerInfo(workerId, workerSocket.getInetAddress().getHostAddress(), (int) registerMsg.getPayload(), out);
            workers.add(info);
            System.out.println("[Master-INFO] Worker " + workerId + " registrado. Total: " + workers.size());
            while (true) {
                Message msg = (Message) in.readObject();
                 if (msg.getType() == Message.MessageType.RESULT) {
                    Object[] payload = (Object[]) msg.getPayload(); int segmentId = (int) payload[0];
                    if (!results.containsKey(segmentId)) {
                        results.put(segmentId, payload[1]);
                        CountDownLatch latch = this.resultLatch;
                        if(latch != null) { latch.countDown(); }
                    }
                }
            }
        } catch (Exception e) {
            if (info != null) {
                synchronized (masterLock) {
                    workers.remove(info);
                    handleFailedWorker(info);
                }
            }
        }
    }

    private void handleFailedWorker(WorkerInfo deadWorker) {
        System.out.println("[Master-CRITICAL] Worker " + deadWorker.getId() + " caído. Iniciando recuperación...");
        
        if (this.resultLatch == null || resultLatch.getCount() == 0) {
            System.out.println("[Master-INFO] No hay operación activa. No se requiere recuperación.");
            return;
        }

        int failedSegmentId = deadWorker.getPrimarySegmentId();
        if (failedSegmentId == -1) {
            System.out.println("[Master-INFO] El worker caído no era primario. No se requiere acción.");
            return;
        }

        String replicaWorkerId = replicaLocations.get(failedSegmentId);
        if (replicaWorkerId == null) { resultLatch.countDown(); return; }

        WorkerInfo backupWorker = workers.stream()
            .filter(w -> w.getId().equals(replicaWorkerId))
            .findFirst().orElse(null);

        if (backupWorker != null) {
            System.out.println("[Master-INFO] Réplica encontrada en " + backupWorker.getId() + ". Reasignando trabajo.");
            Object dataForRecovery = distributedSegments.get(failedSegmentId);
            if(dataForRecovery == null) {
                System.err.println("[Master-FATAL] No se encontró data original para el segmento " + failedSegmentId);
                resultLatch.countDown();
                return;
            }
            // 1. Re-enviar los datos al worker de respaldo
            backupWorker.sendMessage(new Message(Message.MessageType.DATA, new Object[]{failedSegmentId, dataForRecovery}));
            // 2. Darle la orden de computar esos datos
            backupWorker.resetStream(); // Limpiar el stream para evitar problemas de serializacion
            backupWorker.sendMessage(new Message(Message.MessageType.COMPUTE, new Object[]{failedSegmentId, this.lastOperation}));
            backupWorker.setPrimarySegmentId(failedSegmentId);
        } else {
            System.err.println("[Master-FATAL] El worker de la réplica (" + replicaWorkerId + ") también está caído.");
            resultLatch.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Number> T[] consolidateResults(T[] originalArray) {
        // ... (método sin cambios)
        return originalArray;
    }
}