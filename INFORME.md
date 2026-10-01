## Decisiones de diseño para la escalabilidad

El sistema debe poder escalar en cuanto a la cantidad de clientes concurrentes, el volumen de datos que envían y la cantidad de nodos de control.

Para la cantidad de nodos, los Sums funcionan como réplicas sin estado que leen de la misma cola, por lo que agregar uno no requiere reconfigurar nada más que la variable que indica cuántos son. 
En cambio, los Aggregation sí hacen un reparto explícito del trabajo. 
Cada Aggregation se suscribe a su propia clave del exchange, y la clave de destino de cada registro se calcula con un hash del ClientId concatenado con el nombre de la fruta. 
Todas los Sums aplican el mismo cálculo, de modo que cada par de cliente y fruta cae siempre en el misma Aggregation.
Este reparto por hash tiene varios beneficios. Al estar hasheando por fruta, para un mismo cliente las distintas frutas irán a parar a distintos aggregations.
Esto permite que los tops parciales de los distintos Aggregations sean útiles para el cálculo del top final. 
Si en cambio, una misma fruta fuera dividida entre distintos aggregations quizás cada uno de estos nodos interpreten como que tiene una cantidad baja cuando en realidad la suma total era muy alta.
Además, el hasheo por clientId permite que, si una misma fruta es muy popular entre distintos clientes, los agregations puedan balancear la carga.

Para la cantidad de clientes, fue necesario incluir el ClientId en todos los mensajes internos del sistema. 
Esto permitió distinguir entre registros de distintos clientes y así procesar varios clientes en simultáneo, sin que los datos de uno se mezclen con los de otro. 
Como todo el estado de cada nodo está indexado por el ClientId, agregar clientes concurrentes no requiere modificar el diseño.

Otro punto importante es que los mensajes de coordinación son livianos y no mueven información real de frutas de clientes: el mensaje *open* declara una cantidad y los mensajes *count* notifican cantidades. 
El costo de abrir la coordinación de un cliente es el mismo independientemente de cuántos registros haya enviado ese cliente.


## Deciciones de diseño para la coordinación de nodos

Para poder obtener el resultado completo de cada cliente es necesario sincronizar el trabajo realizado por los diferentes nodos del sistema. 
Para resolver esta coordinación se utilizó el EOF.
Cada EOF contiene dos datos importantes: el identificador del cliente al que pertenece y la cantidad total de mensajes que dicho cliente envió al sistema. 
Debido a que los EOF son colocados en una cola compartida por los distintos Sum, cualquiera de ellos puede recibirlo, por lo que se necesitó implementar un mecanismo adicional para notificar al resto de los nodos.
Para esto se incorporó un canal de exchange que permite que los distintos Sum intercambien información sobre el progreso de cada cliente.

El procedimiento utilizado es el siguiente:

1- Inicio de la coordinación: cuando un Sum recibe el EOF correspondiente a un cliente, publica mediante el exchange un mensaje de tipo open. 
Este mensaje incluye el identificador del cliente y el número total de registros que fueron enviados por él. 
El mensaje se distribuye a todos los Sum, ya que todos necesitan conocer el comienzo del cierre de ese cliente.

2- Notificación de registros procesados: al recibir el open, cada Sum informa mediante un mensaje count cuántos registros de ese cliente había procesado hasta ese momento. 
Si todavía no había recibido ninguno, informa un valor de cero. 
A partir de ese instante, cada nuevo registro perteneciente a ese cliente genera otro mensaje count con valor uno.
Estos mensajes también se envían mediante broadcast. 
No existe un Sum encargado de centralizar el conteo, sino que cada nodo mantiene su propio registro de los mensajes procesados y todos reciben las actualizaciones necesarias.

3- Finalización de un cliente: la coordinación puede considerarse terminada cuando la suma de todos los valores informados mediante mensajes count coincide con la cantidad total de registros indicada originalmente en el EOF.
Una vez alcanzada esta condición, cada Sum envía hacia los Aggregation los datos que acumuló para ese cliente y, posteriormente, les envía su correspondiente EOF.
Incluso en el caso de que un determinado Sum no haya procesado ningún registro de ese cliente, igualmente debe enviar el EOF. Esto es necesario porque los Aggregation esperan recibir una señal de finalización proveniente de cada uno de los Sum.

En el lado de los Aggregation, cada nodo mantiene un contador de EOF recibidos para cada cliente. Cuando dicho contador alcanza el número total de Sum existentes, el Aggregation puede asumir que ya recibió toda la información correspondiente a ese cliente. 
En ese momento calcula su top parcial y lo envía al Join.

Finalmente, el Join realiza una coordinación equivalente con los resultados parciales. 
Para cada cliente cuenta cuántos top recibió de los diferentes Aggregation. 
Cuando alcanza la cantidad esperada, combina todos esos resultados parciales y obtiene el top definitivo, que posteriormente se envía al cliente.

Por lo tanto, incluso cuando un Aggregation no tenga información de un determinado cliente, debe enviar igualmente un top vacío al Join. 
De esta manera, el Join puede contabilizar correctamente la participación de todos los Aggregation y determinar cuándo recibió la totalidad de los resultados necesarios.
