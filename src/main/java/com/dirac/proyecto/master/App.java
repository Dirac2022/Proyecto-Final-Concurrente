package com.dirac.proyecto.master;

import java.util.Arrays;
public class App {

    public static void main(String[] args) throws Exception {
        // 1. Iniciar el Master
        Master master = new Master(9090); // El master escucha en el puerto 9090
        master.start();

        // Esperar a que se conecten los workers. En un caso real, esto sería más robusto.
        System.out.println("Esperando 10 segundos para que los workers se conecten...");
        Thread.sleep(20000); 

        // 2. Crear los datos
        int arraySize = 20000; // Un array grande
        double[] myArray = new double[arraySize];
        for (int i = 0; i < arraySize; i++) {
            myArray[i] = i + 1; // Llenar con datos simples 1, 2, 3...
        }
        
        System.out.println("Array original creado con " + arraySize + " elementos.");

        // 3. Crear el array distribuido
        Master.DArrayDouble distributedArray = master.createDArrayDouble(myArray);

        // 4. Ejecutar la operación del Ejemplo 1
        System.out.println("\n--- INICIANDO EJEMPLO 1: PROCESAMIENTO MATEMÁTICO ---");
        long startTime = System.currentTimeMillis();
        
        double[] results = distributedArray.applyOperation("math");
        
        long endTime = System.currentTimeMillis();

        if (results != null) {
            System.out.println("¡Cálculo completado exitosamente!");
            System.out.println("Tiempo de ejecución: " + (endTime - startTime) + " ms");
            // Imprimir una pequeña parte del resultado para verificar
            System.out.println("Primeros 10 resultados: " + Arrays.toString(Arrays.copyOfRange(results, 0, 10)));
            System.out.println("Últimos 10 resultados: " + Arrays.toString(Arrays.copyOfRange(results, results.length - 10, results.length)));
        } else {
            System.err.println("El cálculo falló.");
        }
        
        // El programa principal termina aquí, pero el Master sigue corriendo para aceptar más trabajos.
    }
}
