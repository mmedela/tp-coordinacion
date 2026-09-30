# Coordinacion de Sum y Aggregation

## Flujo general del sistema

```
Cliente --TCP--> Gateway --input_queue--> Sum (N replicas)
                                            |
                                    (hash de fruta) exchange direct
                                            |
                                            v
                                    Aggregation (K replicas)
                                            |
                                        join_queue
                                            |
                                            v
                                        Join --results_queue--> Gateway --TCP--> Cliente
```

Cada cliente se identifica con un `client_id` (UUID) asignado por el `Massagehandler` del Gateway al aceptar la conexion. Ese `client_id` viaja en todos los mensajes internos, por lo que todos los controladores pueden procesar datos de multiples clientes en simultaneo sin mezclarlos. El estado de cada componente es siempre un diccionario indexado por `client_id`

## Coordinacion entre instancias de sum

El problema de fondo es uqe el Gateway reparte los registros de un mismo cliente entre **todas** las replicas de `Sum` a traves de una unica cola compartida y el OEF (`IngestionFinishedMessage`) tambien puede caer en cualquier replica. Para resolver el problema de que las colas no podian saber cuando termino de llegar una infesta o si las otras colas terminaron de procesar las suyas, se implementaron **Colas de control** con nombres derivados de `SUM_PREFIX` y del `ID` de cada replica.

- `<SUM_PREFIX>_coordinator`: cola compartida donde todas las replicas reportan su progreso.

- `<SUM_PREFIX>_flush_<sum_id>`: una cola por replica, usada para ordenarle el flush.

la replica con `ID = 0` actua _coordinadora_ y es la unica que consume `<SUM_PREFIX>_coordinator`. El protocolo es:

1. Cada `Sum` que recibe un dato (`IngestionDataMessage`) lo acumula localmente y publica un `RecordProcessedMessage(client_id, sequence)` en la cola coordinadora. `squence` es un contador que arma el `MessageHandler` del Gateway por conexión, asi que identifica univocamente aca registro de ese cliente

2. El `Sum` que recibe el EOF (`IngestionFinishedMessage`) publica un `IngestionReportedMessage` con la cantidad total de registros enviados por el cliente

3. El coordinador mantiene un conjunto de secuencias reportadas por cada cliente y el `record_count` esperado. Cuando las ecuencias reportadas llegan a `record_count - 1`, save que **todas** las replicas terminaron de aplicar todos los datos de ese cliente, sin importar en que replica cayo cada uno y recien ahi publica un `FlushMessage` en las `N` colas `<SUM_PREFIX>_flush_<sum_id>`

4. Cada replica, al recibir su flush, vuelva sus totales locales de ese cliente hacia Aggregation y se **'olvida'** de su estado (`_handle_flush`)

Este esquema evita el flush prematura, ya que ninguna cierra los totales de un lciente hasta que la barrera del coordinador confirma que no queda ningun dato pendiente en el resto de las replicas.

## Particion hacia Aggregation

El esqueleto original hacia _broadcast_ de cada total a todas las instancias de `Aggregation`, obligando a cada una  a procesar y descartar datos que no le correspondian. En la implementacion actual `aggregator_index_for_fruit` calcula un unduce deterministico por fruta usando `zlib.crc32(fruit)%AGGREGATION:AMOUNT`. Usar `hash()` rompia la particion  porque daba un resultado distinto en cada proceso.

Cada `Sum`, al ahcer flush, envia cada total unicamente al exchange bindeado con la routing key de la `Aggregation` que le corresponde a esa fruta y ademas difunde un `SumFinishedMessage` a **todas** las instancias de `Aggregation`. Ese ultimo mensaje si debe llegar a todas, porque cada una necesita saber que ya recibio todo lo que le tecaba antes de cerrar el top parcial de ese cliente. Esto ayuda a minimizar la cantidad de informacion intercambiada.

`Aggregation` acumula localmente los totales por cliente y fruta y cuenta cuantas replicas de `Sum` (`SUM_AMOUNT`) le avisaron finalizavion. Al llegar la ultima, calcula el top parcial y lo envia a `Join`

## Coordinacion 

`Join` recibe un `PartialRessultMessage` por cada instancia de `Aggregation`. Como el aggregator de origen viaja en el mensaje, `Join` no necesita ningun protocolo de control adicional: simplemente guarda el ultimo top parcial recibido por `aggregator_id` en un diccionario por cliente y en cuanto tiene una entrada por cada una de las `AGGREGATION_AMOUNT` instancias, fusiona los tops parciales, calcula el top final y lo eniva al Gateway.

## Idempotencia ante reentregas

Todas las colas se consumen con ACK manual y rabbitMQ puede reentregar un mensaje confirmado. Para evitar que esas entregas dupliquen computo:

- **`SUM`** guarda el conjunto de `seuqences`s ya aplicadas por cada cliente antes de sumarlas a los totales. Una secueca repetida se descarta

- El coordinador ya era naturalmente idempotente. Agregar 2 veces la misma `squence` a un `set`, o reescribir el mismo `record_count` no cambia el resultado.

- **`Aggregation`** deduplica los totales parciales por la clave y lleva el conteo de `SumFinishedMessages` recibidos como un `set` de `sum_id` en vez de un contador, para uqe un mensaje repetido no infle la cuenta ni dispare un top antes de tiempo. Ademas recuerda los clientes ya cerrados para ignorar mensajes tardios

- **`Join`** es idempotente por construccion. Guarda el top parcial en un diccionario indexado por `aggregator_id`, asi que sobreescribir con el mismo valor no tiene ningun efecto.

## SIGTERM

`Sum`, `Aggregation` y `Join` capturan `SIGTERM` y detiene el consumo de forma prolija antes de salir, en vez de ser terminados abruptamente por Docker. En `Aggregation` y `Join`, que consumen en el mismo hilo donde corre el `signal.signal`, el handler llama directamente a `stop_consuming()`. En `Sum`, corre dos o tres colas en hilos distintos, cada una con su propia conexion de `pika`,  el handler usa `connection.add_callback_threadsafe` para pedirle a cada conexion que se detenga desde el hilo que la creo, que es el unico uso soportado por el adaptador bloqueante de `pika` para interactuar con una conexion desde otro hilo.

## Escalabilidad

- **Clientes**: El Gateway atiende cada conexion en un proceso separado y asigna un `client_id` unico, que se usa para indexar todo el estado interno de `Sum`, `Aggregation` y `Join`, asi que agregar clientes concurrentes no requiere coordinaicon adicional entre ellos. Cada cliente avanza de forma completamente independiente a los demas

- **Volumen de datos**: Los datos nunca se centralizan en un unico lugar antes de sumarse cada `Sum` acumula localmente lo que recibe y solo intercambia con el resto contadores de control (`sequence`, `record_count`), no los datos en si. La barrera de flush es O(1) en tamaño de mensaje por dato ingresado, independientemente de cuantos registros tengan el dataset.

- **Cantidad de controles**: tanto `SUM:AMOUNT` como `AGGREGATOR_AMOUNT` son variables de entorno, ninguna replica asume su cantidad total salvo para saber cuantas confirmaciones esperar y ningun componente depende de nombres de contenedor. Todas las colas se nombran a partir de prefijos, IDs y variables de entorno, facilitando escalar la cantidad de replicas de cada control sin necesidad de modificar codigo.