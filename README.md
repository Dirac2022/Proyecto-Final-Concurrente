Compilar:

```sh
$ javac -d bin src/main/java/com/dirac/proyecto/core/*.java src/main/java/com/dirac/proyecto/worker/*.java src/main/java/com/dirac/proyecto/master/*.java
```

Iniciar el Master:
```sh
java -cp bin com.dirac.proyecto.master.App
```

Iniciar workers
````sh
java -cp bin com.dirac.proyecto.worker.Worker <IP-master> <puerto-master> <puerto-escucha-worker>
# Ejemplo
java -cp bin com.dirac.proyecto.worker.Worker 127.0.0.1 9090 9091
```
