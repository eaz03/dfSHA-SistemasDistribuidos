# DFSha: plan de implementación

Última actualización: 18 de octubre de 2026 (noche).

## 1. Requisitos del enunciado y estado

| Requisito | Cómo lo cubre DFSha | Estado |
|---|---|---|
| Gestión del sistema de archivos (RF1: `ls`, `cd`, `mkdir`, `rmdir`, `rm`...) | Montaje FUSE: los comandos de Linux funcionan sin cambios | ✓ |
| Transferencia y acceso (RF2 `send`/`receive`, RF3 `open`/`read`/`write`) | `cp`, `cat` y editores a través del montaje; los bytes van del cliente a los peers por bloques | Por bloques ✓ (sin FUSE, `dfs_cli`); por el montaje: hito actual |
| Transparencia de acceso y localización | El montaje FUSE hace que DFSha se vea como una carpeta local | Metadatos ✓; contenido: hito actual |
| Particionamiento | Bloques de 1 MiB repartidos por turnos entre los peers | ✓ |
| Alta disponibilidad (15%) | 2 réplicas, heartbeats, lectura con réplica alternativa, re-replicación, varios superpeers, failover | Parcial: réplicas, heartbeats y réplica alternativa ✓; falta re-replicación, varios superpeers y failover |
| Replicación y consistencia (15%) | Commit atómico de la lista de bloques; log de operaciones persistente y replicado entre superpeers | Parcial: commit atómico, limpiador y log persistente ✓; falta replicar el log |
| Escalabilidad (15%) | Superpeer solo con metadatos; datos directo cliente ↔ peer; transferencias en paralelo; más peers = más capacidad | ✓ en diseño y código; falta medirlo |
| Seguridad (15%) | Usuarios (scrypt), login con JWT, un espacio de nombres por usuario, mTLS con roles, cifrado en reposo, permisos firmados por bloque | ✓ (falta: cuotas) |
| Comunicaciones (15%) | gRPC + mTLS en todos los enlaces; el `.proto` es la especificación de la API | ✓ |
| Cada nodo expone una API | Servicios gRPC `Superpeer` y `Peer` | ✓ |
| Opción 2: P2P federado sobre Internet con VPN | Varios superpeers + peers que se comunican entre sí; despliegue en VMs con VPN | Parcial: un superpeer + peers ✓; falta federación, peer ↔ peer y VMs |
| Infraestructura | Docker y Docker Compose; AWS Academy o GCP | ✓ local; VMs pendiente (guía en `docs/DESPLIEGUE.md`) |
| Entregables (10%) | Informe, repositorio documentado, video de 10 a 15 min | README y guía de despliegue ✓; informe y video pendientes |

## 2. Arquitectura

| Componente | Rol | Código |
|---|---|---|
| Cliente | Monta DFSha con FUSE; traduce cada solicitud del kernel a gRPC; bloques directo con los peers | `dfsha/client/`: `mount.py`, `client.py`, `auth_client.py`, `dfs_cli.py` (temporal) |
| Superpeer | Solo metadatos: registro (persistente), tabla de peers, plan y ubicación de bloques, usuarios, limpiador | `dfsha/superpeer/` |
| Peer | Guarda y entrega bloques por ID (cifrados en disco); registro y heartbeats | `dfsha/peer/` |
| Común | Errores, mTLS, tokens JWT, interceptores | `dfsha/common/` |
| Herramientas y pruebas | Certificados, usuarios, estado de peers; pruebas de integración | `tools/`, `tests/` |

Principios: un espacio de nombres por usuario; registro y almacenamiento detrás de interfaces; los bytes nunca pasan por el superpeer.

Flujos con el montaje:

```
Lectura: cat /mnt/dfsha/foto.jpg
 open      -> GetBlockLocations (una vez) -> estado del archivo abierto, caché vacío
 read      -> baja solo los bloques del rango pedido
 read 2.º  -> lectura secuencial: baja el resto en paralelo (archivo <= 64 MiB) o los 8 bloques siguientes
 release   -> descarta el caché

Escritura: cp foto.jpg /mnt/dfsha/
 create    -> CreateFile + archivo temporal local
 write x N -> al temporal (sin red)
 flush     -> upload_from_file: AllocateBlocks, WriteBlock a cada réplica, CommitFile
 release   -> borra el temporal
```

## 3. Implementado (todo en `main`)

| Elemento | Detalle |
|---|---|
| Entorno | Docker en WSL; Compose con `sp1`, `peer1`..`peer3` y `client` |
| Espacio de nombres | `ls`, `cd`, `stat`, `find`, `mkdir`, `rmdir`, `rm`, `mv`, `touch` por FUSE |
| Errores | `FileSystemError` -> gRPC (errno en la metadata) -> FUSE |
| Registro | Interfaz `Registry`, `DictRegistry` y `PersistentRegistry` (log con `fsync` + snapshot, recuperación) |
| Peers | Registro con reintentos, heartbeats, detección de caídas, re-registro, apagado con SIGTERM; `PeerTracker` |
| Bloques | Contrato en el `.proto`; `BlockStorage` en disco y cifrado (AES-256-GCM); `WriteBlock`/`ReadBlock`/`DeleteBlock` |
| Superpeer | `AllocateBlocks`/`CommitFile`/`GetBlockLocations`; réplicas con fallback; solo peers con espacio; limpiador de bloques obsoletos |
| Cliente | Lectura y escritura por bloques, réplica alternativa, `ENODATA`/`EIO`, 4 bloques en paralelo, memoria acotada; `dfs_cli.py` (temporal) |
| Seguridad | Usuarios, login JWT, mTLS con roles, permisos firmados por bloque (vencen a los 30 min), cifrado en reposo |
| Repositorio | `dfsha/` por tipo de nodo, `tools/`, `tests/`, `docs/` (incluye `DESPLIEGUE.md`) |
| Pruebas | `test_peers`, `test_peer_blocks`, `test_superpeer_blocks` |

## 4. Plan restante

### Hito actual: lectura y escritura por el montaje (programa completo básico)

| # | Subtarea | Estado |
|---|---|---|
| 5 | Lectura en el mount: `open`/`read`/`release`, caché por archivo abierto, descarga anticipada (la que falla no hace fallar la lectura actual), nuevo intento con permisos frescos | ✓ (pendiente de commit) |
| 6 | Escritura en el mount: `create`, `open` con escritura (con o sin `O_TRUNC`), `write`, `truncate`, `flush`/`fsync` (subida), `getattr` con el tamaño del temporal, `chmod`/`chown` sin efecto | Siguiente |
| 7 | `tests/test_mount.py`: `cat`, `head`, `tail`, `dd`, `cp` en ambos sentidos, sobrescribir, `>>`, editor, peer caído | Pendiente |
| 8 | Borrar `dfs_cli.py` | Pendiente |

### Después (orden recomendado)

| # | Tarea | Contenido | Criterio |
|---|---|---|---|
| 1 | Despliegue en VMs (paso 6) | 2 o 3 VMs + VPN, un nodo por máquina, certificados por IP (`docs/DESPLIEGUE.md`) | P2P sobre Internet |
| 2 | Comunicación peer ↔ peer (paso 4, B2) | `ReplicateBlock`: el superpeer ordena a un peer con copia enviarla a otro; re-replicación al caer un peer; reporte de bloques al registrarse | Alta disponibilidad, P2P |
| 3 | Varios superpeers (paso 5, B3, B4) | `sp2` se une con una semilla; membresía entre superpeers; replicación del log de operaciones; peers registrados en todos los superpeers; failover del cliente | Alta disponibilidad, consistencia, P2P federado |
| 4 | Pendientes menores | `SetTimes`/`utimens`, `st_blocks` (`du`), `statfs` (`df`), decorador de errores en el mount, SIGTERM en el superpeer, mount multihilo, quitar `--demo`, cuotas | Transparencia, seguridad |
| 5 | Entrega (Fase C) | Pruebas y mediciones, informe (particionamiento, distribución, protocolos), README final, video | Entregables |

### Propiedades evaluadas (Fase B)

| Tarea | Estado |
|---|---|
| B1. Autenticación | ✓ |
| B2. Detección de fallos y re-replicación | Detección ✓; re-replicación pendiente (tarea 2) |
| B3. Persistencia y sincronización del registro | Persistencia ✓; sincronización entre superpeers pendiente (tarea 3) |
| B4. Failover del cliente | Pendiente (tarea 3); hoy `DFSClient` solo recorre la lista de superpeers al conectar |
| B5. Consistencia | Commit atómico, validación de commits y limpiador ✓ |
| B6. Seguridad de transporte y datos | mTLS, cifrado en reposo y permisos por bloque ✓; cuotas pendientes |

## 5. Decisiones

| Decisión | Tomada |
|---|---|
| Tamaño de bloque | 1 MiB |
| Réplicas | 2; si hay menos peers vivos, las que haya (bloque subreplicado) |
| Ubicación | Por turnos entre los peers vivos con espacio para un bloque |
| Lectura en el mount | Bloque a bloque; en la 2.ª lectura consecutiva baja el resto en paralelo (hasta 64 MiB) o una ventana de 8 bloques |
| Escritura en el mount | Sobre un temporal local; se sube completo en `flush` (al cerrar) y en `fsync`; consistencia "close-to-open", como NFS |
| Error al leer un bloque sin réplicas | `ENODATA` ("No data available") |
| Réplica que falla al escribir | Se omite; `EIO` si el bloque no quedó en ninguna |
| Borrado de bloques viejos | Limpiador en segundo plano, con período de gracia |

| Decisión pendiente | Recomendación |
|---|---|
| Sincronización entre superpeers | Un superpeer dueño por usuario aplica las operaciones y envía su log a los demás |
| A qué superpeers se registra cada peer | A todos los de su lista (son pocos): así cada superpeer conoce todos los peers vivos |

## 6. Coordinación

`dfsha/proto/dfsha.proto` es compartido: cada línea agrega sus mensajes y RPC sin renumerar los existentes, y regenera el código gRPC después de cada cambio (y reinicia los contenedores):

```bash
docker compose run --rm sp1 python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfsha/proto/dfsha.proto
docker compose restart
```
