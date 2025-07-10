package com.dirac.proyecto.core;

import com.dirac.proyecto.master.Master;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Representa un array distribuido. Es la interfaz principal para el usuario.
 * Utiliza genéricos para funcionar con cualquier tipo de dato numérico.
 * @param <T> El tipo de número en el array (Integer, Double, etc.).
 */
public class DArray<T extends Number> {
    private final T[] originalArray;
    private final Master master;

    public DArray(T[] array, Master master) {
        this.originalArray = array;
        this.master = master;
    }

    /**
     * Envía una operación para ejecución distribuida y devuelve un Future con el resultado.
     * La segmentación de datos se realiza automáticamente entre los workers disponibles.
     * @param operation El nombre de la operación a ejecutar ("math", "conditional").
     * @return Un objeto Future que contendrá el array de resultados cuando el cálculo termine.
     */
    public Future<T[]> applyOperation(String operation) {
        return master.submitOperation(this.originalArray, operation, null);
    }

    /**
     * Versión sobrecargada para ejecución con SEGMENTACIÓN MANUAL.
     * @param operation El nombre de la operación.
     * @param manualSegments Un mapa que especifica qué rango del array procesará cada worker.
     * @return Un objeto Future que contendrá el array de resultados.
     */
    public Future<T[]> applyOperation(String operation, Map<Integer, int[]> manualSegments) {
        return master.submitOperation(this.originalArray, operation, manualSegments);
    }
}