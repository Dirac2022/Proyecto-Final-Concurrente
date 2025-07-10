package com.dirac.proyecto.master;

import com.dirac.proyecto.core.Message;
import com.dirac.proyecto.core.WorkerInfo;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
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
        final WorkerInfo target; final Message message;
        SendCommand(WorkerInfo target, Message message) { this.target = target; this.message = message; }
    }

    public Master(int port) { this.port = port; }

    // El método ahora declara que puede lanzar excepciones, cumpliendo la regla de Java.
    public void start() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[Master-INFO] Master iniciado en puerto " + port);
            while (true) {
                Socket workerSocket = serverSocket.accept(); // Esta línea ahora es válida.
                new Thread(() -> handleWorker(workerSocket)).start();
            }
        }
    }

    public <T extends Number> Future<T[]> submitOperation(T[] array, String operation, Map<Integer, int[]> manualSegments) {
        Callable<T[]> task = () -> {
            List<SendCommand> commandPlan = planOperation(array, operation, manualSegments);
            if (commandPlan == null) return null; if (commandPlan.isEmpty()) return array;
            
            for (SendCommand cmd : commandPlan) { cmd.target.sendMessage(cmd.message); }
            
            resultLatch.await();
            return consolidateResults(array);
        };
        return executionPool.submit(task);
    }
    
    private <T extends Number> List<SendCommand> planOperation(T[] array, String operation, Map<Integer, int[]> manualSegments) {
        synchronized (masterLock) {
            if (workers.size() < 2) { return null; }
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
                        commands.add(new SendCommand(worker, new Message(Message.MessageType.COMPUTE, new Object[]{worker.getPrimarySegmentId(), operation})));
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
            worker.sendMessage(new Message(Message.MessageType.DATA, new Object[]{i, segment}));
            worker.resetStream();
        }
    }
    
    private void replicateAndPlan(List<WorkerInfo> currentWorkers, List<SendCommand> commands) {
        int numWorkers = currentWorkers.size(); if (numWorkers < 2) return;
        for (int i = 0; i < numWorkers; i++) {
            WorkerInfo primaryWorker = currentWorkers.get(i);
            if (primaryWorker.getPrimarySegmentId() != -1) {
                WorkerInfo replicaWorker = currentWorkers.get((i + 1) % numWorkers);
                int segmentId = primaryWorker.getPrimarySegmentId();
                replicaLocations.put(segmentId, replicaWorker.getId());
                commands.add(new SendCommand(primaryWorker, new Message(Message.MessageType.REPLICATE_ORDER, new Object[]{segmentId, replicaWorker.getAddress(), replicaWorker.getListenPort()})));
            }
        }
    }

    private void handleWorker(Socket workerSocket) {
        WorkerInfo info = null;
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {
            Message registerMsg = (Message) in.readObject();
            info = new WorkerInfo(workerSocket.getInetAddress().getHostAddress() + ":" + workerSocket.getPort(), workerSocket.getInetAddress().getHostAddress(), (int) registerMsg.getPayload(), out);
            workers.add(info);
            System.out.println("[Master-INFO] Worker " + info.getId() + " registrado. Total: " + workers.size());
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
        if (this.resultLatch == null || resultLatch.getCount() == 0) return;
        int failedSegmentId = deadWorker.getPrimarySegmentId();
        if (failedSegmentId == -1) return;
        String replicaWorkerId = replicaLocations.get(failedSegmentId);
        if (replicaWorkerId == null) { resultLatch.countDown(); return; }

        WorkerInfo backupWorker = workers.stream().filter(w -> w.getId().equals(replicaWorkerId)).findFirst().orElse(null);

        if (backupWorker != null) {
            Object dataForRecovery = distributedSegments.get(failedSegmentId);
            if(dataForRecovery == null) { resultLatch.countDown(); return; }
            backupWorker.sendMessage(new Message(Message.MessageType.DATA, new Object[]{failedSegmentId, dataForRecovery}));
            backupWorker.resetStream();
            backupWorker.sendMessage(new Message(Message.MessageType.COMPUTE, new Object[]{failedSegmentId, this.lastOperation}));
            backupWorker.setPrimarySegmentId(failedSegmentId);
        } else {
            resultLatch.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Number> T[] consolidateResults(T[] originalArray) {
        if (results.size() < distributedSegments.size()) return null;
        T[] finalArray = (T[]) Array.newInstance(originalArray.getClass().getComponentType(), originalArray.length);
        for (int i = 0; i < distributedSegments.size(); i++) {
            if (!results.containsKey(i) || !distributedSegments.containsKey(i)) continue;
            Number[] segmentResult = (Number[]) results.get(i);
            int position = 0;
            for(int j = 0; j < i; j++){
                if (distributedSegments.containsKey(j)) {
                   position += Array.getLength(distributedSegments.get(j));
                }
            }
            for (int k = 0; k < segmentResult.length; k++) {
                if (position + k < finalArray.length) {
                    finalArray[position + k] = (T) segmentResult[k];
                }
            }
        }
        return finalArray;
    }
}