# DFSha: plan de implementación

Última actualización: 18 de octubre de 2026.

## 1. Requisitos del enunciado y cómo los cubre el diseño

| Requisito | Cómo lo cubre DFSha | Estado |
|---|---|---|
| Gestión del sistema de archivos (RF1: `ls`, `cd`, `mkdir`, `rmdir`, `rm`...) | Montaje FUSE: los comandos de Linux funcionan sin cambios | ✓ |
| Transferencia y acceso (RF2 `send`/`receive`, RF3 `open`/`read`/`write`) | `cp`, `cat` y editores a través del montaje; los bytes van del cliente a los peers por bloques | Pendiente (pasos 2 a 4) |
| Transparencia de acceso y localización | 100%: el montaje FUSE hace que DFSha se vea como una carpeta local | ✓ (metadatos) |
| Particionamiento: cada archivo repartido en varios nodos | Bloques de tamaño fijo, ubicados en varios peers | Pendiente (pasos 3 y 4) |
| Alta disponibilidad (15%) | k réplicas por bloque, heartbeats, re-replicación, varios superpeers, failover del cliente | Pendiente |
| Replicación y consistencia (15%) | Commit atómico de la lista de bloques; log de operaciones replicado entre superpeers | Pendiente |
| Escalabilidad (15%) | Superpeer solo con metadatos; datos directo cliente ↔ peer; más peers = más capacidad | Diseñado |
| Seguridad (15%) | Usuario/contraseña + token, un espacio de nombres por usuario, TLS/mTLS, cifrado en reposo | Pendiente |
| Comunicaciones (15%) | gRPC en todos los enlaces; el `.proto` es la especificación de la API | ✓ parcial |
| Cada nodo expone una API | Servicios gRPC `Superpeer` y `Peer` | Parcial (contrato del peer preparado) |
| Opción 2: P2P federado sobre Internet con VPN | Superpeers + peers; despliegue en VMs con VPN | Pendiente |
| Infraestructura | Docker y Docker Compose; AWS Academy o GCP | ✓ local, VMs pendiente |
| Entregables (10%) | Informe, repositorio documentado, video de 10 a 15 min | README preliminar ✓ |

## 2. Arquitectura acordada

| Componente | Rol | Archivos |
|---|---|---|
| Cliente | Monta DFSha con FUSE; traduce cada solicitud del kernel a una llamada gRPC | `mount.py`, `client.py` |
| Superpeer | Solo metadatos: registro de archivos y directorios, tabla de peers, ubicación de bloques; nunca ve el contenido | `superpeer.py`, `registry.py`, `dict_registry.py` |
| Peer | Guarda y entrega bloques por ID; se registra en el superpeer y le envía heartbeats | `peer.py` |
| Errores | Un errno exacto viaja desde la regla del registro hasta el kernel | `errors.py` |

Principios:

- Un usuario por espacio de nombres.
- El registro detrás de una interfaz (`Registry`), para poder cambiar su estructura (árbol, persistente, centralizado) sin tocar el superpeer.
- Los datos van directo entre cliente y peer; el superpeer solo entrega IDs y ubicaciones de bloques.
- El mount trabaja sobre una copia temporal del archivo y lo sube completo al cerrarlo.

Flujo de escritura y lectura previsto:

```
Escritura: cp foto.jpg /mnt/dfsha/   (3 MB, bloques de 1 MB)
 1. create          -> superpeer CreateFile (entrada en el registro, tamaño 0); el mount abre un archivo temporal
 2. write x N       -> el mount escribe en el archivo temporal (sin red)
 3. flush (cerrar)  -> superpeer AllocateBlocks(path, 3 MB) -> [(b1, peer1), (b2, peer1), (b3, peer1)]
 4.                 -> el cliente envía b1, b2, b3 directo a peer1 (WriteBlock)
 5.                 -> superpeer CommitFile(path, 3 MB, [b1, b2, b3]): registro actualizado, bloques viejos borrados
 6. release         -> el mount borra el archivo temporal

Lectura: cat /mnt/dfsha/foto.jpg
 1. open            -> superpeer GetBlockLocations(path) -> [(b1, [peer1]), ...]
 2.                 -> el cliente lee b1..b3 de peer1 (ReadBlock) a un archivo temporal
 3. read x N        -> el mount entrega los bytes desde el archivo temporal
 4. release         -> el mount borra el archivo temporal
```

## 3. Implementado hasta ahora

| Elemento | Detalle | Commit |
|---|---|---|
| Entorno | Docker en WSL, `Dockerfile`, `docker-compose.yml` (`sp1` + `client`), generación del código gRPC | ✓ |
| Tarea 1 | `ls`, `cd`, `stat`, `find` de extremo a extremo (`getattr`/`readdir` -> `Stat`/`ListDir`) | ✓ |
| Tarea 2 | `mkdir`, `rmdir`, `rm`, `mv` (con subárbol), `touch` | ✓ |
| Sistema de errores | `FileSystemError` + traducción a gRPC (errno en la metadata) y de vuelta a FUSE | ✓ |
| Registro abstracto | Interfaz `Registry` (contrato documentado) + `DictRegistry`; manejadores de 5 líneas | ✓ |
| README preliminar | En español: cómo ejecutar, desarrollo, problemas comunes | Pendiente |
| Paso 1, subtarea 1 | Contrato de peers en `dfsha.proto` (`RegisterPeer`, `Heartbeat`, `ListPeers`, servicio `Peer` con `Ping`) | Pendiente |
| Paso 1, subtarea 2 | `peer_tracker.py`: tabla de peers en memoria, vivo/muerto por timeout, segura entre hilos | Pendiente |
| Paso 1, subtarea 3 | Manejadores `RegisterPeer` (verifica la dirección con `Ping`), `Heartbeat` y `ListPeers` en `superpeer.py` | Pendiente |
| Paso 1, subtarea 4 | `peer.py`: servidor con `Ping`, registro con reintentos, heartbeats con espacio libre, re-registro, apagado ordenado con SIGTERM | Pendiente |
| Paso 1, subtarea 5 | Servicio `peer1` en `docker-compose.yml` con el volumen `peer1-data` para sus bloques | Pendiente |
| Paso 1, subtarea 6 | `peers_status.py` (tabla de peers) y `test_peers.py` (prueba automática: registro, SIGTERM, caída, regreso, reinicio del superpeer) | ✓ |
| Bloques: almacenamiento (Sofía) | `block_storage.py` (interfaz) + `disk_block_storage.py` (un archivo por bloque, escritura atómica, IDs validados) | ✓ |
| Bloques: registro (Sofía) | `BlockInfo`, `allocate_blocks` / `commit_file` / `get_block_locations`, bloques obsoletos, vencimiento de planes, registro conectado al `PeerTracker` | ✓ |
| Correcciones de bloques | `peer_ids` en vez de direcciones; réplicas con los peers que haya (fallback); commit con las réplicas que sí se escribieron; `peer_ids` como tuple aunque lleguen como lista; solo peers con espacio libre; disco lleno -> `ENOSPC`; `peer2` y `peer3` en Compose | Pendiente |
| Lectura, subtarea 1 | Contrato de bloques en `dfsha.proto`: `WriteBlock`/`ReadBlock`/`DeleteBlock` (peer), `GetBlockLocations`/`AllocateBlocks`/`CommitFile` (superpeer) | Pendiente |
| Lectura, subtarea 2 | `peer.py` sirve `WriteBlock`/`ReadBlock`/`DeleteBlock` sobre `DiskBlockStorage`; `test_peer_blocks.py` | Pendiente |
| Lectura, subtarea 3 | Superpeer: `GetBlockLocations` (peer_id -> dirección, vivos primero), `AllocateBlocks`, `CommitFile`; `test_superpeer_blocks.py` | Pendiente |
| Lectura, subtarea 4 | `client.py`: lectura y escritura por bloques (réplica alternativa, `ENODATA`/`EIO`, 4 bloques en paralelo, memoria acotada); `dfs_cli.py put/get/blocks` (temporal) | Pendiente |

## 4. Plan restante

### Fase A: conectar todos los componentes con funcionalidad mínima

El camino crítico es 1 -> 2 -> 3 -> 4: cada paso usa lo que construyó el anterior.

| Paso | Subtareas | Depende de |
|---|---|---|
| **1. Conexión peer ↔ superpeer** | (1) contrato en `dfsha.proto` ✓ · (2) `peer_tracker.py`: tabla de peers, vivo/muerto por timeout ✓ · (3) manejadores `RegisterPeer`/`Heartbeat`/`ListPeers` ✓ · (4) `peer.py`: servidor, registro con reintentos, heartbeats, re-registro ✓ · (5) `peer1` en Compose con su volumen ✓ · (6) verificar caídas y reinicios ✓ | Nada |
| **2. Almacenamiento en el peer** | `block_storage.py` (interfaz + disco local, un archivo por bloque, escritura a nombre temporal y luego renombrar) · `WriteBlock`/`ReadBlock`/`DeleteBlock` con streaming · espacio libre en el heartbeat · prueba con script, sin FUSE | Paso 1 |
| **3. Lectura y escritura de extremo a extremo** | **Hito actual, lectura primero (de abajo hacia arriba):** (1) contrato en el `.proto` ✓ · (2) `ReadBlock`/`WriteBlock` en el peer ✓ · (3) `GetBlockLocations`/`AllocateBlocks`/`CommitFile` en el superpeer ✓ · (4) `download` + `upload` mínimo en el cliente y `dfs_cli.py put/get` ✓ · (5) `open`/`read`/`release` en el mount · (6) `test_read.py`. Después:  Registro con `block_ids` y `commit_file` · `AllocateBlocks`/`CommitFile`/`GetBlockLocations` · `upload`/`download` en el cliente · `open`/`read`/`write`/`truncate`/`flush`/`release` con archivo temporal · borrar bloques viejos · pruebas con `cp`, `cat`, editores, archivo de varios bloques | Pasos 1 y 2 |
| **4. Varios peers** | `peer2`/`peer3` · política de ubicación · k réplicas · lectura con réplica alternativa · reportes de bloques al registrarse · re-replicación al caer un peer · transferencias en paralelo | Paso 3 |
| **5. Segundo superpeer** | `sp2` se une con una semilla: lista de miembros + copia del registro | Persistencia (B3) para la sincronización real |
| **6. Despliegue preliminar** | 2 o 3 VMs, VPN, contenedores en máquinas distintas | Nada (lo antes posible) |

### Fase B: propiedades evaluadas

| Tarea | Contenido | Criterio |
|---|---|---|
| B1. Autenticación | Login, token en cada RPC, un registro por usuario (reemplaza `DEMO_USER`), credenciales al montar | Seguridad |
| B2. Detección de fallos y re-replicación | Sobre los heartbeats del paso 1 y las réplicas del paso 4 | Alta disponibilidad |
| B3. Persistencia y sincronización del registro | Log de operaciones + snapshot; el mismo log se replica a los otros superpeers (el parámetro `now` del registro ya lo permite) | Consistencia, alta disponibilidad |
| B4. Failover del cliente | Reintentar con otro superpeer si el actual no responde | Alta disponibilidad |
| B5. Consistencia | Reglas de commit, limpieza de bloques huérfanos | Consistencia |
| B6. Seguridad de transporte y datos | TLS/mTLS, cifrado en reposo, validación de entradas, cuotas | Seguridad |

### Fase C: entrega

Despliegue final, pruebas y mediciones, informe (incluidos los algoritmos de particionamiento y distribución), README final, video.

### Pendientes menores (no bloquean nada)

- Borrado de bloques obsoletos y de planes vencidos: cola + hilo en el superpeer que llama a `expire_pending_blocks` y envía `DeleteBlock` (necesita `DeleteBlock` en el peer).

- Decorador de errores en `mount.py` (quitar los `try/except` repetidos).
- `st_blocks` en `getattr`, para que `du` muestre tamaños.
- `utimens`, para que `touch` sobre un archivo existente actualice su fecha.
- Inversión de dependencias del registro: fábrica + `registry: Registry` en el superpeer (en espera).
- Registro en árbol (diferido; solo afecta a la implementación del registro).
- Quitar `--demo` cuando ya no haga falta.
- Borrar `dfs_cli.py` cuando el mount escriba (es temporal, para tener datos que leer).
- Manejador de SIGTERM en `superpeer.py` (como en `peer.py`): hoy `docker compose stop sp1` espera 10 s y lo mata con SIGKILL.

## 5. Decisiones pendientes

| Decisión | Recomendación |
|---|---|
| Tamaño de bloque | 1 MB en desarrollo, configurable |
| Cuándo sube el archivo el mount | En `flush` (al cerrar) |
| Borrado de bloques viejos | Inmediato en el commit por ahora; limpieza periódica después |
| Factor de replicación k | 2; si hay menos peers vivos, se usan los que haya (bloque subreplicado) |
| Política de ubicación | Round robin entre los peers vivos con espacio para un bloque, sin repetir peer en un mismo bloque |
| Cuándo baja el mount los bloques al leer | Bloque a bloque al leer (caché por archivo abierto); si la lectura es secuencial, baja todos sus bloques en paralelo, hasta 64 MiB; en archivos más grandes, los 8 bloques siguientes a lo que se lee. Por confirmar: que la descarga completa empiece en la segunda lectura consecutiva |
| Error al leer un bloque sin réplicas disponibles | `ENODATA` ("No data available"): el archivo existe, sus datos no están disponibles |
| Réplica que falla al escribir | Se omite y el commit va con las réplicas que sí quedaron (bloque subreplicado); `EIO` si no quedó en ninguna |
| Sincronización entre superpeers | Un dueño por usuario que envía su log a los demás |

## 6. Trabajo en paralelo

Fuera del camino crítico (Fase A, pasos 1 a 4), estas líneas pueden avanzar al mismo tiempo sin bloquearse:

| Línea | Qué incluye | Por qué es independiente | Cruce con el camino crítico |
|---|---|---|---|
| **A. Datos** (camino crítico) | Pasos 1 a 4 | | |
| **B. Autenticación** (B1) | Login, tokens, registro por usuario | Solo toca los manejadores del superpeer, `client.py` y el arranque del mount | Los RPC de bloques del paso 3 también deberán llevar el token |
| **C. Persistencia del registro** (B3, primera mitad) | Log de operaciones + snapshot dentro de una implementación de `Registry` | Queda detrás de la interfaz: el superpeer no cambia | Al llegar el paso 3, el log debe registrar también `commit_file` |
| **D. Infraestructura** (paso 6) | VMs, VPN, Docker en las VMs | No depende del código | Probar cada paso terminado en las VMs |
| **E. Seguridad de transporte** (B6) | Certificados, canales TLS/mTLS en gRPC | Es configuración de canales, sin lógica de negocio | Se aplica a cada enlace nuevo |
| **F. Pendientes menores** | Decorador, `st_blocks`, `utimens`, inversión de dependencias | Cambios pequeños y aislados | Ninguno |
| **G. Informe y pruebas** | Arquitectura, diagramas, especificación de protocolos (el `.proto`), script de pruebas funcionales | Documenta y prueba lo que ya existe | Se actualiza con cada paso |

**Coordinación:** `dfsha.proto` es compartido por todas las líneas. Cada una agrega sus propios mensajes y RPC sin renumerar los existentes, y regenera el código gRPC después de cada cambio:

```bash
docker compose run --rm sp1 python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfsha/proto/dfsha.proto
```
