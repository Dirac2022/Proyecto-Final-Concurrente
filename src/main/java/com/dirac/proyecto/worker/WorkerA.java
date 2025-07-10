package com.dirac.proyecto.worker;

import com.dirac.proyecto.core.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerA {

    private final String masterAddress; // Dirección IP del Master
    private final int masterPort; // Puerto del Master
    // listenPort es el puerto en el que este Worker escucha conexiones de otros Workers
    // que envían datos de réplica. Este puerto es utilizado para recibir réplicas
    // de otros Workers, lo que permite que este Worker mantenga una copia actualizada.
    // También es el puerto que se registra con el Master para que este pueda enviarle datos.
    private final int listenPort; // Puerto de escucha del Worker para recibir réplicas.
    private ObjectOutputStream masterOut; // Salida hacia el Master
    // Estructuras para almacenar segmentos primarios y réplicas, es decir, los datos que maneja este Worker
    // Usamos ConcurrentHashMap para permitir acceso concurrente y evitar problemas de sincronización
    // Estos mapas almacenan los segmentos primarios y las réplicas que este Worker maneja.
    // Los segmentos primarios son aquellos que este Worker recibe directamente del Master,
    // mientras que las réplicas son segmentos que este Worker recibe de otros Workers.
    // Estos segmentos se identifican por un ID entero, y el valor asociado puede ser cualquier objeto,
    // que en este caso se espera que sea un array de tipo T (donde T extiende Number).
    // Esto permite que el Worker maneje diferentes tipos de datos numéricos de manera flexible.
    private final Map<Integer, Object> primarySegments = new ConcurrentHashMap<>(); 
    private final Map<Integer, Object> replicaSegments = new ConcurrentHashMap<>();

    public WorkerA(String masterAddress, int masterPort, int listenPort) {
        this.masterAddress = masterAddress;
        this.masterPort = masterPort;
        this.listenPort = listenPort;
    }

    // Lo que hace este método es iniciar el Worker, creando un hilo para escuchar conexiones de réplicas
    // y estableciendo una conexión con el Master. Luego, envía un mensaje de registro
    // y comienza a escuchar comandos del Master.
    // El Worker se ejecuta en un hilo separado para no bloquear el hilo principal.
    // Este método también inicia un hilo para enviar "heartbeat" al Master cada 5 segundos
    // para indicar que el Worker sigue activo.
    // Además, el Worker escucha comandos del Master para recibir datos, órdenes de replicación,
    // promover réplicas a primarios y realizar computaciones sobre los segmentos primarios.
    // // En resumen, este método configura el Worker para que esté listo para recibir y procesar
    // // datos del Master y de otros Workers, gestionando tanto los segmentos primarios como las réplicas.
    // La diferencia entre los segmentos primarios y las réplicas es que los primarios son aquellos
    // que el Worker recibe directamente del Master y son considerados los datos originales,
    // mientras que las réplicas son copias de esos datos que el Worker recibe de otros Workers. 
    public void start() throws Exception {
        // Iniciar el servidor de réplicas en un hilo separado
        // Esto permite que el Worker escuche conexiones entrantes de otros Workers
        // que envían datos de réplica, mientras también se conecta al Master.
        new Thread(this::startReplicaServer).start();
        // Conectar al Master y enviar un mensaje de registro
        // Esto establece una conexión de socket con el Master en la dirección y puerto especificados.|
        Socket masterSocket = new Socket(masterAddress, masterPort);
        // Crear flujos de entrada y salida para comunicarse con el Master
        // Estos flujos se utilizan para enviar y recibir mensajes entre el Worker y el Master.
        // El ObjectOutputStream se utiliza para enviar objetos serializados al Master,
        // mientras que el ObjectInputStream se utiliza para recibir objetos serializados del Master.
        masterOut = new ObjectOutputStream(masterSocket.getOutputStream());
        ObjectInputStream masterIn = new ObjectInputStream(masterSocket.getInputStream());
        // Enviar un mensaje de registro al Master con el puerto de escucha del Worker
        // Este mensaje indica al Master que este Worker está listo para recibir datos y órdenes.
        masterOut.writeObject(new Message(Message.MessageType.REGISTER, listenPort));
        // Lo que hace flush() es asegurarse de que todos los datos enviados al Master. Es decir,
        // lo que ocurre es que el Worker envía un mensaje de registro al Master
        // indicando que está listo para recibir datos y órdenes. Luego, se llama a flush()
        // para asegurarse de que el mensaje se envíe inmediatamente y no se quede en el búfer.
        masterOut.flush();
        // Iniciar el hilo de heartbeat para enviar señales periódicas al Master
        startHeartbeat(masterSocket);
        // Escuchar comandos del Master en un hilo separado
        listenForMasterCommands(masterIn);
    }
    
    // Este método inicia un servidor de sockets en el puerto especificado por listenPort.
    // El Worker escucha conexiones entrantes de otros Workers que envían datos de réplica.
    // Cuando un Worker se conecta, se crea un nuevo hilo para manejar los datos de réplica.
    // Este enfoque permite que el Worker maneje múltiples conexiones de réplicas simultáneamente,
    // lo que es útil en un entorno distribuido donde varios Workers pueden enviar datos de réplica al mismo tiempo.
    // Si ocurre un error al iniciar el servidor, el Worker se detiene inmediatamente con System.exit(1).
    // Esto es importante para evitar que el Worker continúe funcionando en un estado inconsistente si no puede escuchar réplicas.
    // En resumen,  este método configura el Worker para recibir datos de réplica de otros Workers
    // y maneja esas conexiones de manera concurrente, asegurando que los datos de réplica se procesen correctamente.
    private void startReplicaServer() {
        // Crear un servidor de sockets que escucha en el puerto listenPort
        // Este puerto es utilizado por otros Workers para enviar datos de réplica a este Worker.
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            System.out.println("Worker escuchando réplicas en puerto " + listenPort);
            while (true) { // Aceptar conexiones entrantes de otros Workers
                // Cuando un Worker se conecta, se crea un nuevo hilo para manejar los datos de réplica.
                // Esto permite que el Worker maneje múltiples conexiones de réplicas simultáneamente
                // sin bloquear el hilo principal. Cada conexión se maneja en un hilo separado.
                Socket sourceWorkerSocket = serverSocket.accept();
                new Thread(() -> handleReplicaData(sourceWorkerSocket)).start();
            }
        } catch (IOException e) { // Manejar errores de conexión al iniciar el servidor
            // Si ocurre un error al iniciar el servidor, detener el Worker.
            System.exit(1); 
        }
    }

    // Este método maneja los datos de réplica recibidos de otros Workers.
    // Se espera que los datos lleguen en forma de un objeto Message serializado.
    // El método lee el objeto Message del flujo de entrada del socket y verifica si es del tipo REPLICA_DATA.
    // Si es así, extrae el ID del segmento y los datos asociados del payload del mensaje.
    // Luego, almacena los datos de réplica en el mapa replicaSegments, utilizando el ID del segmento como clave.
    // Esto permite que el Worker mantenga una copia actualizada de los segmentos que recibe de otros Workers.
    // Si ocurre un error durante la lectura del objeto, se ignora el error, ya que puede ser causado por problemas de
    // conexión entre Workers que no son críticos para el funcionamiento del Worker.
    // En resumen, este método permite que el Worker reciba y almacene datos de réplica de otros Workers,
    // asegurando que tenga una copia actualizada de los segmentos que maneja.
    private void handleReplicaData(Socket socket) {
        // in es un ObjectInputStream que se utiliza para leer objetos serializados del socket.
        // socket.getInputStream() obtiene el flujo de entrada del socket,
        // que es donde otros Workers envían sus datos de réplica.
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            // La siguiente línea lee un objeto del flujo de entrada y lo convierte a Message.
            // Se espera que este objeto sea un mensaje de tipo REPLICA_DATA que contiene los
            // datos de réplica enviados por otro Worker.
            Message msg = (Message) in.readObject();
            if (msg.getType() == Message.MessageType.REPLICA_DATA) {
                // Aqui extraemos el payload del mensaje, que se espera que sea un array de objetos.
                // El payload contiene el ID del segmento y los datos asociados.
                // El ID del segmento es un entero que identifica de manera única el segmento,
                // y los datos asociados son el objeto que representa el segmento replicado.
                // En este caso, se espera que el objeto sea un array de tipo T (donde T extiende Number).
                // El Worker almacena estos datos en el mapa replicaSegments,
                // utilizando el ID del segmento como clave. Esto permite que el Worker mantenga
                // una copia actualizada de los segmentos que recibe de otros Workers.
                Object[] payload = (Object[]) msg.getPayload();
                // payload[0] es el ID del segmento, y payload[1] es el objeto que contiene 
                // los datos del segmento.
                // El Worker almacena estos datos en el mapa replicaSegments, utilizando el ID del segmento
                // como clave. Esto permite que el Worker mantenga una copia actualizada de los segmentos
                // que recibe de otros Workers.
                replicaSegments.put((Integer) payload[0], payload[1]);
                System.out.println("Réplica para segmento " + payload[0] + " almacenada.");
            }
        } catch (Exception e) {  }
    }

    private void listenForMasterCommands(ObjectInputStream in) {
        try {
            while (true) {
                Message msg = (Message) in.readObject();
                 switch (msg.getType()) {
                    case DATA:
                        Object[] payload = (Object[]) msg.getPayload();
                        primarySegments.put((Integer) payload[0], payload[1]);
                        System.out.println("Segmento primario " + payload[0] + " recibido.");
                        break;
                    case REPLICATE_ORDER:
                        handleReplicateOrder((Object[]) msg.getPayload());
                        break;
                    case PROMOTE_REPLICA:
                        int segId = (Integer) msg.getPayload();
                        if (replicaSegments.containsKey(segId)) {
                            primarySegments.put(segId, replicaSegments.remove(segId));
                            System.out.println("¡PROMOVIDO! Segmento " + segId + " es ahora primario.");
                        }
                        break;
                    case COMPUTE:
                        performComputation((Object[]) msg.getPayload());
                        break;
                    default:
                }
            }
        } catch (Exception e) { 
            System.err.println("Conexión con el Master perdida: " + e.getMessage());
            System.exit(1); 
        }
    }
    
    // Este método maneja la orden de replicación recibida del Master.
    // Se espera que el payload del mensaje contenga un array de objetos con la siguiente estructura:
    // - payload[0]: ID del segmento a replicar (entero)
    // - payload[1]: Dirección IP del Worker secundario (String)
    // - payload[2]: Puerto del Worker secundario (entero)
    // El Worker intenta establecer una conexión con el Worker secundario utilizando la dirección y el puerto
    // proporcionados. Si la conexión es exitosa, envía un mensaje de tipo REPLICA_DATA al Worker secundario,
    // que contiene el ID del segmento y los datos asociados.
    // Si ocurre un error al intentar establecer la conexión o enviar los datos, se ignora el error,
    // ya que la replicación es una operación que puede fallar sin afectar el funcionamiento del Worker.
    // En resumen, este método permite que el Worker envíe datos de réplica a otro Worker secundario,
    // asegurando que los segmentos primarios se mantengan actualizados en múltiples Workers.
    // Esto es parte del mecanismo de replicación activa que permite la resiliencia y disponibilidad
    // de los datos en un sistema distribuido.
    private void handleReplicateOrder(Object[] payload) {
        new Thread(() -> {
            try {
                int segmentId = (int) payload[0];
                String replicaAddress = (String) payload[1];
                int replicaPort = (int) payload[2];
                Object dataToReplicate = primarySegments.get(segmentId);
                if (dataToReplicate == null) return;
                try (Socket replicaSocket = new Socket(replicaAddress, replicaPort);
                    ObjectOutputStream replicaOut = new ObjectOutputStream(replicaSocket.getOutputStream())) {
                    replicaOut.writeObject(new Message(Message.MessageType.REPLICA_DATA, new Object[]{segmentId, dataToReplicate}));
                    System.out.println("Segmento " + segmentId + " replicado en " + replicaAddress + ":" + replicaPort);
                }
            } catch (Exception e) { 
                System.err.println("Fallo al replicar segmento: " + e.getMessage());
            }
        }).start();
    }

    // Este método realiza la computación sobre los datos del segmento primario.
    // Se espera que el payload del mensaje contenga un array de objetos con la siguiente estructura:
    // - payload[0]: ID del segmento a procesar (entero)
    // - payload[1]: Tipo de operación a realizar (String), puede ser "math" o "conditional"
    // El Worker obtiene el segmento primario correspondiente al ID proporcionado y
    // lo convierte a un array de tipo T (donde T extiende Number).
    // Luego, crea un array de resultados del mismo tamaño que el segmento primario.
    // Utiliza un ThreadPoolExecutor para paralelizar la computación, creando un hilo
    // por cada elemento del segmento primario. Cada hilo realiza la operación especificada:
    // - Si la operación es "math", calcula una expresión matemática basada en el valor del elemento.
    // - Si la operación es "conditional", aplica una condición sobre el valor del elemento
    //   y realiza un cálculo basado en esa condición.
    // Si ocurre un error durante la computación (por ejemplo, división por cero o logaritmo de cero),
    // se captura la excepción y se imprime un mensaje de error, continuando con el siguiente elemento.
    // Finalmente, el Worker envía los resultados de la computación
    // de vuelta al Master en un mensaje de tipo RESULT, que contiene el ID del segmento
    // y el array de resultados.
    // Por ejemplo si data.length es 1000, se crean 1000 hilos para procesar cada elemento del segmento.
    // Lo cual esta mal, ya que crear un hilo por cada elemento puede ser ineficiente y consumir muchos recursos.
    // En su lugar, se utiliza un ThreadPoolExecutor para manejar un número fijo de hilos,
    // lo que permite que el Worker procese múltiples elementos en paralelo sin crear demasiados hilos.
    // Esto mejora la eficiencia y reduce el consumo de recursos del sistema.
    // Ahora si data.length es 1000 y hay 8 nucleos, por tanto cores es 8, el funcioamiento es el siguiente:
    // - Se crea un ThreadPoolExecutor con 8 hilos.
    // - Cada hilo toma un elemento del segmento primario y realiza la operación especificada.
    // - Los hilos se ejecutan en paralelo, procesando hasta 8 elementos al mismo tiempo.
    // - Una vez que todos los hilos han terminado, se espera a que terminen con executor.awaitTermination(1, TimeUnit.HOURS).
    // - Finalmente, se envían los resultados de la computación al Master.
    // Pero uno se pregunta, si hay 1000 elementos y 8 hilos, ¿cómo se distribuyen los elementos entre los hilos?
    // La distribución de los elementos entre los hilos se realiza de la siguiente manera:
    // - Cada hilo toma un elemento del segmento primario en un bucle for.
    // - El índice del elemento se almacena en una variable final llamada index.
    // - Cuando se llama a executor.submit(), cada hilo recibe su propio índice y procesa el elemento correspondiente.
    // - Por ejemplo, el hilo 1 procesará el elemento en el índice 0, el hilo 2 procesará el elemento en el índice 1, y así sucessivamente.
    // - Esto permite que cada hilo procese un elemento diferente del segmento primario
    //   sin interferir con los demás hilos.
    // - Al final, todos los hilos han procesado sus respectivos elementos y se obtiene un array de resultados
    //   que contiene los resultados de la computación para cada elemento.
    // - Por ejemplo, si el segmento primario tiene 1000 elementos, se crean 1000 tareas,
    //   cada una procesando un elemento diferente del segmento primario.
    // - Esto permite que el Worker realice la computación de manera eficiente y en paralelo
    //   sin crear demasiados hilos, lo que podría agotar los recursos del sistema. Si el hilo 1 ya completo si tarea, entonces
    //   puede tomar el siguiente elemento del segmento primario y procesarlo, permitiendo que
    //   el Worker aproveche al máximo los recursos disponibles.
    // Un resumen de como funciona, se tiene imaginemos algo mas sencillo: 20 datos y 4 hilos. El paso a paso
    // es el siguiente:
    // 1. Se crea un ThreadPoolExecutor con 4 hilos.
    // 2. Se itera sobre los 20 datos, y para cada dato se crea una tarea que se envía al executor.
    // 3. Cada tarea toma un dato y realiza la operación especificada.
    // 4. Los hilos del ThreadPoolExecutor procesan las tareas en paralelo.
    // 5. Si un hilo termina su tarea antes que los demás, puede tomar la siguiente tarea disponible.
    // 6. Una vez que todas las tareas han sido enviadas, se espera a que todos los hilos terminen con `executor.awaitTermination(1, TimeUnit.HOURS)`.
    // 7. Finalmente, se envían los resultados de la computación al Master en un mensaje de tipo RESULT.
    @SuppressWarnings("unchecked") // Para linea de casting de Object a T[]
    private <T extends Number> void performComputation(Object[] payload) throws Exception {
        // payload es un array de objetos que contiene el ID del segmento y la operación a realizar.
        // payload[0] es el ID del segmento (entero) y payload[1] es la operación a realizar (String).
        int segmentId = (int) payload[0];
        String operation = (String) payload[1];
        // Aqui obtenemos el segmento primario correspondiente al ID proporcionado.
        // primarySegments es un mapa que almacena los segmentos primarios, donde la clave es el ID del segmento
        // y el valor es el objeto que contiene los datos del segmento.
        Object dataObject = primarySegments.get(segmentId);
        if (dataObject == null) {
            System.out.println("Error: No tengo datos primarios para el segmento " + segmentId + " para computar.");
            return; // Si el segmento no existe, no se puede realizar la computación.
        }
        // Hacemos un casting de dataObject a un array de tipo T.
        T[] data = (T[]) dataObject; 
        // Aqui creamos un array de resultados del mismo tamaño que el segmento primario.
        // Este array se utilizará para almacenar los resultados de la computación.
        Number[] results = new Number[data.length];
        // Creamos un ThreadPoolExecutor con un número fijo de hilos igual al número de núcleos disponibles.
        int cores = Runtime.getRuntime().availableProcessors();
        // Aqui creamos un ThreadPoolExecutor con un número fijo de hilos igual al número de núcleos disponibles.
        // Esto permite que el Worker procese múltiples elementos en paralelo sin crear demasiados hilos
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores);
        // El resumen del funcionamiento del bucle for es el siguiente:
        // - Iteramos sobre cada elemento del segmento primario (data).
        // - Para cada elemento, creamos una tarea que se envía al ThreadPoolExecutor
        // - Cada tarea toma el elemento correspondiente del segmento primario y realiza la operación especificada
        // - Utilizamos una variable final llamada index para capturar el índice del elemento actual.
        // - Esto permite que cada hilo procese un elemento diferente del segmento primario
        //   sin interferir con los demás hilos.
        // - Al final, todos los hilos han procesado sus respectivos elementos y se obtiene
        //   un array de resultados que contiene los resultados de la computación para cada elemento.
        // - Por ejemplo, si el segmento primario tiene 1000 elementos, se crean 1000 tareas,
        //   cada una procesando un elemento diferente del segmento primario.
        // - Esto permite que el Worker realice la computación de manera eficiente y en paralelo
        //   sin crear demasiados hilos, lo que podría agotar los recursos del sistema.
        // - Si un hilo ya ha completado su tarea, puede tomar la siguiente tarea disponible
        //   del segmento primario y procesarla, permitiendo que el Worker aproveche al máximo los recursos disponibles.
        for (int i = 0; i < data.length; i++) {
            // index es una variable final que captura el índice del elemento actual.
            // Esto es necesario para que cada hilo pueda acceder al índice correcto del elemento
            // que está procesando, ya que las variables locales en un lambda deben ser efectivamente
            // finales o ser declaradas como final.
            final int index = i;
            // Enviamos una tarea al ThreadPoolExecutor para procesar el elemento en el índice actual.
            // Cada tarea se ejecuta en un hilo del ThreadPoolExecutor, lo que permite que
            // múltiples elementos se procesen en paralelo.
            // executor.submit() envía una tarea al ThreadPoolExecutor para que sea ejecutada por
            // uno de los hilos disponibles. La tarea es un Runnable que contiene la lógica de
            // computación para el elemento en el índice actual.
            executor.submit(() -> {
                try {
                    // Aqui obtenemos el elemento del segmento primario en el índice actual.
                    // data es un array de tipo T (donde T extiende Number) que contiene
                    // los datos del segmento primario.
                    T x = data[index]; 
                    if ("math".equals(operation)) {
                        results[index] = Math.pow(Math.sin(x.doubleValue()) + Math.cos(x.doubleValue()), 2) / (Math.sqrt(Math.abs(x.doubleValue())) + 1);
                    } else if ("conditional".equals(operation)) {
                        int val = x.intValue();
                        if (val % 3 == 0 || (val >= 500 && val <= 1000)) {
                            if (val == 0) throw new ArithmeticException("log(0)");
                            results[index] = (int) (val * Math.log(val)) % 7;
                        } else {
                            results[index] = val;
                        }
                    }
                }
                // Las excepciones que pueden ocurrir son:
                // - ArithmeticException: Si se intenta dividir por cero o calcular el logaritmo de cero.
                // - NullPointerException: Si el segmento primario es nulo o si el elemento en el índice actual es nulo.
                // - ArrayIndexOutOfBoundsException: Si el índice está fuera de los límites del array.
                // - ClassCastException: Si el casting de dataObject a T[] falla. 
                catch (Exception e) {
                    System.err.println("Algun error: " + e.getMessage() + ". Continuando...");
                    results[index] = -1; // Marcar error
                }
            });
        }
        // Aqui esperamos a que todas las tareas del ThreadPoolExecutor terminen.
        // Lo que hace shutdown() especificamente es que no se aceptan nuevas tareas,
        // pero las tareas que ya han sido enviadas se completan.
        executor.shutdown();
        // awaitTermination() espera a que todas las tareas se completen o hasta que se alcance el tiempo máximo especificado.
        // En este caso, se espera hasta 1 hora para que todas las tareas terminen.
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("Computación (" + operation + ") terminada para segmento " + segmentId);
        masterOut.writeObject(new Message(Message.MessageType.RESULT, new Object[]{segmentId, results}));
        // Lo que hace flush() es asegurarse de que todos los datos enviados al Master.
        masterOut.flush();
    }

    // Este metodo inicia un hilo que envía un "heartbeat" al Master cada 5 segundos.
    // El "heartbeat" es un mensaje que indica que el Worker sigue activo y funcionando.
    // Esto es importante para que el Master pueda detectar si un Worker ha fallado o está
    // inactivo. Si el Master no recibe un "heartbeat" de un Worker en un tiempo determinado,
    // puede asumir que el Worker ha fallado y tomar las medidas necesarias, como
    // redistribuir los segmentos que ese Worker estaba manejando.
    private void startHeartbeat(Socket masterSocket) {
        Thread t = new Thread(() -> {
            while (!masterSocket.isClosed()) {
                try {
                    Thread.sleep(5000);
                    masterOut.writeObject(new Message(Message.MessageType.HEARTBEAT, null));
                    masterOut.flush();
                } catch (Exception e) { break; }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java com.dirac.proyecto.worker.Worker <master_ip> <master_port> <my_listen_port>");
            return;
        }
        try {
            // Inicia el Worker con la dirección del Master, el puerto del Master y
            // el puerto de escucha del Worker para recibir réplicas.
            // args[0] es la dirección IP del Master, args[1] es el puerto del Master,
            // y args[2] es el puerto en el que este Worker escucha conexiones de réplicas.
            new WorkerA(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2])).start(); 
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }
}