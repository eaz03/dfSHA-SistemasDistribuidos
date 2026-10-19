# Resultados de las mediciones

Mediciones de DFSha en el despliegue local con Docker Compose (18 de octubre de 2026), con los scripts
de esta carpeta. Cada tabla dice con qué script se obtuvo, para poder repetirla.

## Entorno

| | |
|---|---|
| Máquina | Laptop, AMD Ryzen 5 5625U (6 núcleos, 12 hilos), 5,8 GB de RAM, SSD NVMe Micron 512 GB |
| Sistema | Windows 11 con Docker Desktop sobre WSL2 (kernel 6.6), Docker 29.8 |
| Recursos de Docker | 12 CPUs y **2,8 GB de RAM** para todos los contenedores |
| Despliegue | 1 superpeer, 3 peers y 1 cliente, cada uno en su contenedor, en la misma máquina |
| Configuración | Bloques de 1 MiB, k = 2 réplicas, mTLS en todos los canales, bloques cifrados con AES-256-GCM, `fsync` en cada bloque y en cada modificación del registro |

**Lo que hay que tener en cuenta al leer los números:** todos los nodos comparten **una sola
CPU, un solo disco y poca memoria**, y la "red" es la red virtual de Docker dentro de la misma
máquina. Por eso no hay latencia de red real, pero los tres peers compiten por el mismo disco
cuando escriben las réplicas con `fsync`. En máquinas separadas, cada peer tendría su propio disco
y el throughput de escritura escalaría con la cantidad de peers; la red sería el nuevo límite.

## 1. Transferencia de archivos

`docker compose exec client python -m benchmarks.bench_transfer`

**Según el tamaño** (4 bloques en paralelo; mediana de 5, entre paréntesis el mínimo y el máximo):

| Tamaño | put: s (mín–máx) | put (MiB/s) | get: s (mín–máx) | get (MiB/s) |
|---|---|---|---|---|
| 1 MiB | 0.24 (0.15–0.70) | 4.1 | 0.06 (0.02–0.15) | 16.1 |
| 10 MiB | 0.34 (0.28–0.59) | 29.4 | 0.14 (0.12–0.25) | 73.5 |
| 50 MiB | 2.27 (2.11–19.42) | 22.0 | 1.33 (0.73–2.90) | 37.7 |
| 100 MiB | 12.33 (7.04–21.38) | 8.1 | 5.41 (3.12–11.23) | 18.5 |

**Efecto del paralelismo** (archivo de 50 MiB, mediana de 5):

| Bloques en paralelo | put: s (mín–máx) | put (MiB/s) | get: s (mín–máx) | get (MiB/s) |
|---|---|---|---|---|
| 1 | 10.02 (6.01–29.49) | 5.0 | 2.39 (1.31–3.86) | 21.0 |
| 2 | 7.73 (5.85–19.48) | 6.5 | 2.15 (1.80–8.93) | 23.3 |
| 4 | 4.26 (3.60–5.25) | 11.7 | 1.98 (0.59–3.06) | 25.2 |
| 8 | 2.98 (1.42–6.19) | 16.8 | 1.90 (1.26–2.38) | 26.4 |

**Análisis**

- **Los archivos pequeños pagan un costo fijo.** Un `put` de 1 MiB tarda ~0,24 s aunque sea un
  solo bloque: incluye crear la entrada, pedir el plan, subir el bloque a 2 peers (cada uno lo
  cifra y hace `fsync`) y el commit (con su `fsync` del log). Por eso el throughput crece mucho
  entre 1 y 10 MiB: el costo fijo se reparte.
- **Leer es 2 a 4 veces más rápido que escribir.** Al escribir, cada bloque va a 2 peers y cada
  uno hace `fsync`; al leer, se baja una sola réplica y el bloque suele estar en la caché de
  memoria del sistema operativo.
- **El paralelismo funciona.** Subir 4 bloques a la vez en lugar de 1 más que duplica el
  throughput de escritura (5,0 → 11,7 MiB/s), y 8 lo triplica: mientras un peer hace `fsync`, el
  cliente ya está enviando otros bloques a otros peers. En lectura la mejora es menor (21 → 26
  MiB/s) porque leer ya es rápido y el cuello de botella pasa a ser la CPU compartida (descifrar y
  TLS).
- **La variación es grande en archivos grandes** (de 7 a 21 s para 100 MiB). Con 2,8 GB de RAM
  para todos los contenedores y un solo disco, un archivo de 100 MiB escrito dos veces (200 MiB
  con `fsync`) satura la caché y el disco virtual de WSL2, y además el limpiador borra en segundo
  plano los bloques de mediciones anteriores. Por eso el throughput baja de 50 a 100 MiB: es un
  límite del entorno de prueba, no del diseño.

## 2. Operaciones de metadatos

`docker compose exec client python -m benchmarks.bench_metadata`

| Operación | ops/s | mediana (ms) | p95 (ms) |
|---|---|---|---|
| mkdir (modifica: fsync) | 192 | 4.7 | 9.8 |
| create (modifica: fsync) | 238 | 3.9 | 5.8 |
| stat (consulta) | 688 | 1.4 | 1.7 |
| ls (consulta) | 604 | 1.6 | 2.0 |
| rm (modifica: fsync) | 235 | 4.0 | 5.9 |

**Análisis.** Las consultas tardan ~1,5 ms: es el costo de una llamada gRPC con mTLS y token, más
la búsqueda en memoria. Las modificaciones tardan unos 2,5 ms más, que es el `fsync` del log de la
persistencia: lo que cuesta que "ok" signifique "ya está en disco". Un solo cliente hace unas 200
modificaciones por segundo; el límite del superpeer es parecido, porque todas las modificaciones
pasan por el mismo lock y el mismo log (agruparlas en un solo `fsync` lo multiplicaría).

## 3. Costo de la seguridad y de la persistencia

`docker compose run --rm sp1 python -m benchmarks.bench_internals`

**Cifrado en reposo** (100 bloques de 1 MiB, con `fsync`):

| Almacenamiento | escritura (MiB/s) | lectura (MiB/s) |
|---|---|---|
| disco, sin cifrar | 75 | 4526 |
| disco + AES-256-GCM | 54 | 327 |

**Canal gRPC** (100 bloques de 1 MiB, un cliente, sin disco):

| Canal | MiB/s |
|---|---|
| sin TLS | 206 |
| mTLS | 186 |

**Persistencia del registro:**

| Archivos en el registro | snapshot (ms) | tamaño (MB) | una modificación con fsync (ms) |
|---|---|---|---|
| 1,000 | 12 | 0.07 | 2.8 |
| 10,000 | 102 | 0.72 | 2.8 |
| 50,000 | 168 | 3.70 | 2.6 |

**Análisis**

- **mTLS cuesta ~10 %** del throughput del canal (206 → 186 MiB/s). Es poco frente a lo que
  protege, y el canal sigue siendo mucho más rápido que el disco.
- **El cifrado en reposo cuesta ~28 % al escribir** (75 → 54 MiB/s): el `fsync` sigue dominando.
  En lectura la diferencia parece enorme (4526 → 327 MiB/s), pero los 4526 MiB/s sin cifrar son
  bloques que se leen de la caché de memoria, sin trabajo de CPU; con cifrado, descifrar y verificar
  cada bloque es el trabajo real. Aun así, 327 MiB/s es más que lo que entrega la red en el sistema
  completo (sección 1), así que no es el cuello de botella.
- **Un snapshot es barato.** Con 10 000 archivos tarda 0,1 s y se toma cada 1 000 modificaciones:
  repartido, cuesta ~0,1 ms por modificación, frente a los ~2,8 ms del `fsync`. Esto justifica el
  valor de 1 000. El tiempo de una modificación no depende del tamaño del registro.

## 4. Alta disponibilidad

`python3 -m benchmarks.bench_availability` (fuera de los contenedores; tiempos reales del sistema)

| Medición | Resultado | Explicación |
|---|---|---|
| Detección de un peer caído | 6.5 s | Muerto 9 s después de su último heartbeat, que pudo llegar hasta 3 s antes de la caída |
| Re-replicación completa de sus bloques | 78.6 s desde la caída | 9 s para darlo por muerto + 60 s de retardo + pasadas cada 10 s |
| Superpeer atendiendo tras un reinicio | 1.8 s | Arranque + reconstrucción del registro desde el snapshot y el log |
| Archivo legible e idéntico tras el reinicio | 7.7 s | Espera a que los peers se vuelvan a registrar |

**Análisis.** Mientras un peer está caído, **las lecturas no se interrumpen** (se lee de la otra
réplica; ver `tests/integration/test_failures.py`). Los 78,6 s hasta recuperar las 2 réplicas son
casi todos **espera intencional**: el retardo de 60 s evita copiar todos los bloques de un peer que
solo se está reiniciando. Es un parámetro (`--replication-delay`): bajarlo acorta la ventana en que
un bloque tiene una sola copia, a cambio de copias innecesarias. El superpeer vuelve a atender en
menos de 2 s y sin perder nada; los 6 s siguientes son los peers volviendo a registrarse.
