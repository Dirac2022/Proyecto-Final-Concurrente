package com.dirac.proyecto.core;

import java.io.Serializable;

// Hacemos la clase 'Serializable' para poder enviarla a través de la red.
public class Message implements Serializable {
    // Esto define los tipos de mensajes que pueden ser enviados entre el Master y los Workers.
    // Cada tipo de mensaje tiene un propósito específico, como registrar un Worker, enviar datos,
    // realizar una operación de computación, o manejar la replicación activa.
    public enum MessageType {
        // Worker a Master
        REGISTER, // Worker -> Master: Registro del Worker con su puerto de escucha
        HEARTBEAT, // Worker -> Master: Indica que el Worker sigue activo
        RESULT, // Worker -> Master: Resultado de una operación
        
        // Master a Worker
        REGISTER_OK,  // Master -> Worker: Confirmación de registro
        DATA, // Master -> Worker: Envío de datos para procesar
        COMPUTE, // Master -> Worker: Orden de computación sobre un segmento
        
        // --- Comandos para Replicación Activa ---
        REPLICATE_ORDER,  // Master -> Primario: Ordena replicar un segmento a un secundario
        REPLICA_DATA,     // Primario -> Secundario: Envío de los datos de la réplica
        PROMOTE_REPLICA   // Master -> Secundario: Orden para que una réplica se vuelva primaria
    }

    private final MessageType type;
    private final Object payload;

    public Message(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }
    public MessageType getType() { return type; }
    public Object getPayload() { return payload; }
}