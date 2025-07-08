package com.dirac.proyecto.core;

import java.io.Serializable;

// Hacemos la clase 'Serializable' para poder enviarla a través de la red.
public class Message implements Serializable {

    public enum MessageType {
        // Worker a Master
        REGISTER,       // Un worker se registra
        HEARTBEAT,      // "Estoy vivo"
        RESULT,         // Devuelve el resultado de un cálculo

        // Master a Worker
        REGISTER_OK,    // El master confirma el registro
        DATA,           // Envío de un segmento de datos
        REPLICATE,      // Orden para replicar un segmento en otro worker
        COMPUTE,        // Orden para empezar a calcular
        PROMOTE_REPLICA // Orden para que una réplica se vuelva primaria
    }

    private final MessageType type;
    private final Object payload; // El contenido del mensaje (datos, texto, etc.)

    public Message(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", payload=" + payload +
                '}';
    }
}