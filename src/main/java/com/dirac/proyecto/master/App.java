package com.dirac.proyecto.master;

import com.dirac.proyecto.core.DArray;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class App {

    public static void main(String[] args) {
        Master master = new Master(9090);
        Thread masterThread = new Thread(() -> {
            try {
                master.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        masterThread.setDaemon(true);
        masterThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Master iniciado. Inicia los workers en otras terminales.");
        System.out.println("Una vez que todos se hayan registrado, presiona ENTER aquí.");
        System.out.println("--------------------------------------------------------------");
        new Scanner(System.in).nextLine();

        
        // EJEMPLO 1: Procesamiento Matemático
        System.out.println("\n--------------------------------------------------------------");
        System.out.println("\nEjemplo 1: Procesamiento Matemático Asíncrono");
        try {
            Double[] doubleArray = new Double[100000];
            for (int i = 0; i < doubleArray.length; i++) doubleArray[i] = (double) i;
            DArray<Double> distDoubleArray = new DArray<>(doubleArray, master);

            Future<Double[]> futureResult = distDoubleArray.applyOperation("math");
            System.out.println("Operación enviada. El cliente ahora espera el resultado...");
            
            Double[] finalResult = futureResult.get(); 
            
            if (finalResult != null) {
                System.out.println("Ejemplo 1 completado. Resultado recibido.");
                System.out.println("Primeros 5 resultados: " + Arrays.toString(Arrays.copyOfRange(finalResult, 0, 5)));
            } else {
                System.err.println("El Ejemplo 1 falló. Revisa los logs del Master.");
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error durante la ejecución del Ejemplo 1: " + e.getMessage());
        }
        


        // EJEMPLO 2: Evaluación Condicional Resiliente
        System.out.println("\n--------------------------------------------------------------"); 
        System.out.println("\nEjemplo 2");
        try {
            Integer[] intArray = new Integer[1001]; // Tamaño 1001 para incluir el 1000
            for (int i = 0; i < intArray.length; i++) intArray[i] = i;
            DArray<Integer> distIntArray = new DArray<>(intArray, master);

            Future<Integer[]> futureResult = distIntArray.applyOperation("conditional");
            System.out.println("Operación enviada. El cliente ahora espera el resultado...");

            Integer[] finalResult = futureResult.get();

            if (finalResult != null) {
                System.out.println("Ejemplo 2 completado. Resultado recibido.");
                System.out.println("Revisa los logs de los workers para ver el manejo del error local en 'log(0)'.");
                System.out.println("Resultado para el valor 0 (causó error): " + finalResult[0]);
                System.out.println("Resultado para el valor 3 (multiplo): " + finalResult[3]);
                System.out.println("Resultado para el valor 499 (no en rango): " + finalResult[499]);
                System.out.println("Resultado para el valor 500 (en rango): " + finalResult[500]);
                System.out.println("Resultado para el valor 501 (en rango): " + finalResult[501]);
            } else {
                 System.err.println("El Ejemplo 2 falló. Revisa los logs del Master.");
            }
        } catch (InterruptedException | ExecutionException e) {
             System.err.println("Error durante la ejecución del Ejemplo 2: " + e.getMessage());
        }
        

        
        // EJEMPLO 3: Simulación de Fallo y Recuperación
        System.out.println("\n--------------------------------------------------------------");
        System.out.println("\nEjemplo 3");
        try {
            int arraySize = 500000;
            System.out.println("Creando un array grande de " + arraySize + " elementos...");
            Double[] doubleArray = new Double[arraySize];
            for (int i = 0; i < doubleArray.length; i++) doubleArray[i] = (double) i;
            DArray<Double> distDoubleArray = new DArray<>(doubleArray, master);
            
            Future<Double[]> futureResult = distDoubleArray.applyOperation("math");
            System.out.println("Operación enviada y En curso.");
            
            System.out.println("Esperando 5 segundos para que el cómputo se estabilice...");
            Thread.sleep(5000);

            System.out.println("CERRAR worker de un terminal (simula fallo)");
            System.out.println("(Revisa los logs del Master para ver a quién se le asignaron los segmentos).");
            System.out.println("Presiona Enter aquí después de haber cerrado la terminal del worker.");
            new Scanner(System.in).nextLine();

            System.out.println("Esperando el resultado final (puede tardar si hubo recuperacion)...");
            Double[] finalResult = futureResult.get();

            if (finalResult != null) {
                 System.out.println("Ejemplo 3 completado. El sistema se recuperó del fallo y finalizo el calculo.");
            } else {
                 System.err.println("El Ejemplo 3 fallo. La recuperación no fue exitosa.");
            }
        } catch (InterruptedException | ExecutionException e) {
             System.err.println("Error durante la ejecución del Ejemplo 3: " + e.getMessage());
        }
        

        System.out.println("\n--------------------------------------------------------------");
        System.out.println("\nFin del programa cliente");
    }
}