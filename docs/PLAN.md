# DFSha: plan de implementación

Última actualización: 18 de octubre de 2026 (noche, después de la escritura por el montaje y `test_mount.py`).

## 1. Requisitos del enunciado y estado

| Requisito | Cómo lo cubre DFSha | Estado |
|---|---|---|
| Gestión del sistema de archivos (RF1: `ls`, `cd`, `mkdir`, `rmdir`, `rm`...) | Montaje FUSE: los comandos de Linux funcionan sin cambios | ✓ |
| Transferencia y acceso (RF2 `send`/`receive`, RF3 `open`/`read`/`write`) | `cp`, `cat`, `>`, `>>` y editores a través del montaje; los bytes van del cliente a los peers por bloques | ✓ |
| Transparencia de acceso y localización | El montaje FUSE hace que DFSha se vea como una carpeta local | ✓ (lectura y escritura) |
| Particionamiento | Bloques de 1 MiB repartidos por turnos entre los peers | ✓ |
| Alta disponibilidad (15%) | 2 réplicas, heartbeats, lectura con réplica alternativa, re-replicación, varios superpeers, failover | Parcial: réplicas, heartbeats, réplica alternativa y re-replicación ✓; faltan varios superpeers y failover |
| Replicación y consistencia (15%) | Commit atómico de la lista de bloques; close-to-open en el montaje; log de operaciones persistente y replicado entre superpeers | Parcial: commit atómico, close-to-open, limpiador y log persistente ✓; falta replicar el log |
| Escalabilidad (15%) | Superpeer solo con metadatos; datos directo cliente ↔ peer y peer ↔ peer; transferencias en paralelo; más peers = más capacidad | ✓ en diseño y código; falta medirlo |
| Seguridad (15%) | Usuarios (scrypt), login con JWT, un espacio de nombres por usuario, mTLS con roles, cifrado en reposo, permisos firmados por bloque | ✓ (falta: cuotas) |
| Comunicaciones (15%) | gRPC + mTLS en todos los enlaces; el `.proto` es la especificación de la API | ✓ |
| Cada nodo expone una API | Servicios gRPC `Superpeer` y `Peer` | ✓ |
| Opción 2: P2P federado sobre Internet con VPN | Varios superpeers + peers que se comunican entre sí; despliegue en VMs con VPN | Parcial: un superpeer, peers y peer ↔ peer (re-replicación) ✓; faltan federación de superpeers y VMs |
| Infraestructura | Docker y Docker Compose; AWS Academy o GCP | ✓ local; VMs pendiente (guía en `docs/DESPLIEGUE.md`) |
| Entregables (10%) | Informe, repositorio documentado, video de 10 a 15 min | README y guía de despliegue ✓; informe y video pendientes |

## 2. Arquitectura

| Componente | Rol | Código |
|---|---|---|
| Cliente | Monta DFSha con FUSE; traduce cada solicitud del kernel a gRPC; bloques directo con los peers | `dfsha/client/`: `mount.py`, `client.py`, `auth_client.py`, `dfs_cli.py` (respaldo sin FUSE) |
| Superpeer | Solo metadatos: registro (persistente), tabla de peers, plan y ubicación de bloques, usuarios, limpiador, monitor de réplicas | `dfsha/superpeer/` (`__main__.py` es el punto de entrada: `python -m dfsha.superpeer`) |
| Peer | Guarda y entrega bloques por ID (cifrados en disco); copia bloques desde otro peer cuando el superpeer lo ordena; registro y heartbeats | `dfsha/peer/` (`__main__.py`: `python -m dfsha.peer`) |
| Común | Errores, mTLS y creación de servidores (`tls.py`), tokens JWT, interceptores | `dfsha/common/` |
| Herramientas y pruebas | Certificados, usuarios, estado de peers; pruebas de integración | `tools/`, `tests/` |

Principios: un espacio de nombres por usuario; registro y almacenamiento detrás de interfaces; los bytes nunca pasan por el superpeer.

Flujos con el montaje:

```
Lectura: cat /mnt/dfsha/foto.jpg
 open        -> GetBlockLocations (una vez) -> estado del archivo abierto (ReadHandle), caché vacío
 read        -> baja solo los bloques del rango pedido
 read 2.º    -> lectura secuencial: baja el resto en paralelo (archivo <= 64 MiB) o los 8 bloques siguientes
 release     -> descarta el caché

Escritura: cp foto.jpg /mnt/dfsha/
 create      -> CreateFile + copia local vacía (WriteHandle)
 open (W)    -> si el archivo ya existe: copia local vacía; su contenido se baja solo si hace falta (>>, editar el medio)
 truncate 0  -> el kernel lo envía justo después de open en cp y >: no se baja nada
 write x N   -> a la copia local, sin red (pedazos de 128 KiB con big_writes)
 flush       -> upload_from_file: AllocateBlocks, WriteBlock a cada réplica, CommitFile
 release     -> borra la copia local

Re-replicación (superpeer, en segundo plano)
 cada 10 s   -> bloques con menos de 2 réplicas (peer muerto hace más de 60 s)
 ReplicateBlock -> el peer destino copia el bloque directo desde un peer que lo tiene
 registro    -> anota la réplica nueva (queda en el log)
```

## 3. Implementado (en `main` o pendiente de commit)

| Elemento | Detalle |
|---|---|
| Entorno | Docker en WSL; Compose con `sp1`, `peer1`..`peer3` y `client` |
| Espacio de nombres | `ls`, `cd`, `stat`, `find`, `mkdir`, `rmdir`, `rm`, `mv`, `touch` por FUSE |
| Montaje: lectura | Bloque a bloque, caché por archivo abierto, descarga anticipada opcional, nuevo intento con permisos frescos, `ENODATA` |
| Montaje: escritura | Copia local por archivo abierto, subida al cerrar, carga perezosa, `truncate`, `getattr` con el tamaño de la copia, `mv`/`rm` con el archivo abierto, `chmod`/`chown`/`utimens` sin efecto, `big_writes` |
| Errores | `FileSystemError` -> gRPC (errno en la metadata) -> FUSE |
| Registro | Interfaz `Registry`, `DictRegistry` y `PersistentRegistry` (log con `fsync` + snapshot, recuperación) |
| Peers | Registro con reintentos, heartbeats, detección de caídas, re-registro, apagado con SIGTERM; `PeerTracker` |
| Bloques | Contrato en el `.proto`; `BlockStorage` en disco y cifrado (AES-256-GCM); `WriteBlock`/`ReadBlock`/`DeleteBlock`/`ReplicateBlock` |
| Superpeer | `AllocateBlocks`/`CommitFile`/`GetBlockLocations`; réplicas con fallback; solo peers con espacio; limpiador de bloques obsoletos; monitor de re-replicación |
| Cliente | Lectura y escritura por bloques, réplica alternativa, `ENODATA`/`EIO`, 4 bloques en paralelo, memoria acotada |
| CLI de respaldo | `dfs_cli.py` (`put`, `get`, `blocks`) sin FUSE: si el montaje falla, para diagnosticar, para mostrar las réplicas de cada bloque y para verificar en `test_mount.py` |
| Servidores gRPC | `create_secure_server` (`tls.py`): solo mTLS y sin `SO_REUSEPORT`; un segundo proceso en el mismo puerto falla al arrancar |
| Seguridad | Usuarios, login JWT, mTLS con roles, permisos firmados por bloque (vencen a los 30 min), cifrado en reposo |
| Repositorio | `dfsha/` por tipo de nodo, `tools/`, `tests/`, `docs/` (incluye `DESPLIEGUE.md`) |
| Pruebas | `test_peers`, `test_peer_blocks`, `test_superpeer_blocks`, `test_mount` (13 casos del montaje) |

Pendiente de commit: `dfsha/client/mount.py` (escritura), `dfsha/client/client.py` (comentario), `dfsha/client/dfs_cli.py` (docstring), `dfsha/common/tls.py`, `dfsha/peer/__main__.py`, `dfsha/superpeer/__main__.py`, `tests/test_mount.py`, `README.md`, este plan.

## 4. Plan restante

### Hito actual: lectura y escritura por el montaje (programa completo básico)

| # | Subtarea | Estado |
|---|---|---|
| 5 | Lectura en el mount | ✓ |
| 6 | Escritura en el mount (probada a mano con FUSE real: `cp` de 2,5 a 100 MiB en ambos sentidos, `>`, `>>`, `truncate`, `sed -i`, `vim`, `tar`, archivo abierto con `mv`/`rm`, peer caído, sin peers) | ✓ (pendiente de commit) |
| 7 | `tests/test_mount.py`: lo anterior como prueba automática (13 casos: lectura, escritura, archivo abierto, fallas), al estilo de `test_peers.py`: en WSL, `docker compose exec client` para los comandos y `docker compose stop` para las fallas | ✓ (pendiente de commit; falta correrla con Docker) |
| 8 | `dfs_cli.py`: se queda como respaldo sin FUSE, documentado en el README y en el módulo | ✓ |

### Después (orden recomendado)

| # | Tarea | Contenido | Criterio |
|---|---|---|---|
| 1 | Despliegue en VMs (paso 6) | 2 o 3 VMs + VPN, un nodo por máquina, certificados por IP (`docs/DESPLIEGUE.md`) | P2P sobre Internet |
| 2 | Varios superpeers (paso 5, B3, B4) | `sp2` se une con una semilla; membresía entre superpeers; replicación del log de operaciones; peers registrados en todos los superpeers; failover del cliente | Alta disponibilidad, consistencia, P2P federado |
| 3 | Pendientes menores | Reporte de bloques del peer al registrarse (hoy se asume que conserva todos), guardar fechas (`SetTimes`; hoy `utimens` no hace nada), `st_blocks` (`du`), `statfs` (`df`), decorador de errores en el mount, SIGTERM en el superpeer, mount multihilo, quitar `--demo`, cuotas | Transparencia, alta disponibilidad, seguridad |
| 4 | Entrega (Fase C) | Pruebas y mediciones, informe (particionamiento, distribución, protocolos), README final, video | Entregables |

### Propiedades evaluadas (Fase B)

| Tarea | Estado |
|---|---|
| B1. Autenticación | ✓ |
| B2. Detección de fallos y re-replicación | ✓ (monitor de réplicas, copia peer ↔ peer) |
| B3. Persistencia y sincronización del registro | Persistencia ✓; sincronización entre superpeers pendiente (tarea 2) |
| B4. Failover del cliente | Pendiente (tarea 2); hoy `DFSClient` solo recorre la lista de superpeers al conectar |
| B5. Consistencia | Commit atómico, validación de commits, limpiador y close-to-open en el montaje ✓ |
| B6. Seguridad de transporte y datos | mTLS, cifrado en reposo y permisos por bloque ✓; cuotas pendientes |

## 5. Decisiones

| Decisión | Tomada |
|---|---|
| Tamaño de bloque | 1 MiB |
| Réplicas | 2; si hay menos peers vivos, las que haya (bloque subreplicado, que luego completa la re-replicación) |
| Ubicación | Por turnos entre los peers vivos con espacio para un bloque |
| Lectura en el mount | Bloque a bloque; en la 2.ª lectura consecutiva baja el resto en paralelo (hasta 64 MiB) o una ventana de 8 bloques |
| Escritura en el mount | Copia local por archivo abierto; se sube completa al cerrar (`flush`) y en `fsync`; el contenido actual se baja solo si hace falta; consistencia close-to-open, como NFS |
| Errores de la subida | Llegan en `close`: `cp`, Python y `vim` los muestran; `echo >` de bash no revisa `close` |
| Permisos, dueño y fechas | No se guardan: `chmod`, `chown` y `utimens` se aceptan sin efecto |
| CLI sin FUSE | `dfs_cli.py` se queda como respaldo (no se borra): diagnóstico, réplicas por bloque, verificación en las pruebas |
| Error al leer un bloque sin réplicas | `ENODATA` ("No data available") |
| Réplica que falla al escribir | Se omite; `EIO` si el bloque no quedó en ninguna |
| Re-replicación | Peer muerto más de 60 s; destino: el peer vivo con más espacio que no tenga el bloque; copia peer a peer; máximo 10 copias por pasada |
| Borrado de bloques viejos | Limpiador en segundo plano, con período de gracia |

| Decisión pendiente | Recomendación |
|---|---|
| Sincronización entre superpeers | Un superpeer dueño por usuario aplica las operaciones y envía su log a los demás |
| A qué superpeers se registra cada peer | A todos los de su lista (son pocos): así cada superpeer conoce todos los peers vivos |
| Peer que se bloquea con dos SIGTERM seguidos | En el manejador, ignorar las señales siguientes antes de activar `stop_event` |
| `SEQUENTIAL_READS_BEFORE_PREFETCH` | 3 en vez de 2: un `dd` corto que cruza un bloque no bajaría el archivo entero |

## 6. Coordinación

`dfsha/proto/dfsha.proto` es compartido: cada línea agrega sus mensajes y RPC sin renumerar los existentes, y regenera el código gRPC después de cada cambio (y reinicia los contenedores):

```bash
docker compose run --rm sp1 python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfsha/proto/dfsha.proto
docker compose restart
```
