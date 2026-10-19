# DFSha

Sistema de archivos distribuido (DFS) basado en bloques, organizado como una red P2P con superpeers.
Proyecto 1 de Sistemas Distribuidos (SI3007), Universidad EAFIT, 2026-2.

> **Estado: funcional con un superpeer.** DFSha se monta con FUSE y se usa como una carpeta común:
> espacio de nombres (`ls`, `cd`, `mkdir`, `rm`, `mv`...), lectura (`cat`, `less`, `cp` desde el montaje)
> y escritura (`cp` hacia el montaje, `>`, `>>`, editores). Los archivos se parten en bloques de 1 MiB con
> 2 réplicas en los peers: si un peer cae, se lee de la otra réplica y el superpeer copia sus bloques a
> otro peer. Todo va con login, mTLS y bloques cifrados en disco.
> Faltan varios superpeers (federación y failover) y el despliegue en VMs: ver [docs/PLAN.md](docs/PLAN.md).

## Estructura

```
dfsha/                  el código del sistema: un subpaquete por tipo de nodo
  proto/                dfsha.proto: contrato gRPC de todos los servicios (el código generado no va a git)
  common/               lo que comparten los nodos: errores, mTLS, tokens JWT, claves, interceptores
  superpeer/            superpeer (python -m dfsha.superpeer): registro de metadatos, tabla de peers, usuarios
  peer/                 peer (python -m dfsha.peer): guarda y entrega bloques (cifrados en disco)
  client/               cliente: DFSClient, montaje FUSE (python -m dfsha.client.mount) y CLI de respaldo sin FUSE
tools/                  administración: certificados, usuarios, estado de los peers (python -m tools.<nombre>)
tests/                  pruebas de integración sobre los contenedores (python -m tests.<nombre>)
docs/                   arquitectura, plan de trabajo y guía de despliegue
Dockerfile, docker-compose.yml   imagen y contenedores: sp1, peer1..peer3 y client
```

Todo se ejecuta **desde la raíz del repositorio** con `python -m`, para que los imports
(`from dfsha.common.errors import ...`) funcionen igual en cualquier máquina.

| Módulo | Rol |
|---|---|
| `dfsha/client/mount.py` | Monta DFSha como una carpeta común (FUSE): cualquier programa (`ls`, `cd`, `mkdir`...) la usa sin saber que es remota |
| `dfsha/client/client.py` | `DFSClient`: habla con el superpeer (metadatos) y directo con los peers (bloques) |
| `dfsha/client/dfs_cli.py` | Respaldo sin FUSE: subir (`put`), bajar (`get`) y ver los bloques y sus réplicas (`blocks`). Para cuando el montaje falla, para diagnosticar si un problema es de FUSE o del resto del sistema, y para las pruebas (`test_mount.py`) |
| `dfsha/superpeer/__main__.py` | Servidor gRPC del superpeer: login, operaciones del registro, registro de peers, plan y ubicación de bloques |
| `dfsha/superpeer/registry.py` | Contrato (interfaz) del registro de metadatos; `dict_registry.py` lo implementa en memoria |
| `dfsha/superpeer/persistent_registry.py` | Guarda el registro en disco: log de operaciones (con `fsync`) + snapshot; al arrancar lo reconstruye |
| `dfsha/superpeer/peer_tracker.py` | Tabla de peers: dirección, espacio libre, vivo o muerto según sus heartbeats |
| `dfsha/superpeer/block_cleaner.py` | Borra en segundo plano los bloques obsoletos (sobrescritos, borrados, planes vencidos), después de un período de gracia y reintentando en peers caídos |
| `dfsha/superpeer/replication_monitor.py` | Devuelve a cada bloque las réplicas perdidas: si un peer muere (o se escribió con pocos peers), ordena copiar sus bloques de peer a peer (`ReplicateBlock`) |
| `dfsha/superpeer/user_store.py` | Usuarios y contraseñas (hash scrypt); `json_user_store.py` los guarda en un JSON |
| `dfsha/peer/__main__.py` | Peer: se registra, envía heartbeats y atiende `WriteBlock`/`ReadBlock`/`DeleteBlock` con permisos firmados |
| `dfsha/peer/block_storage.py` | Interfaz del almacenamiento de bloques; `disk_block_storage.py` (disco) y `encrypted_block_storage.py` (AES-256-GCM) |
| `dfsha/common/errors.py` | Errores de sistema de archivos y su traducción entre capas (registro, red, kernel) |
| `dfsha/common/tls.py`, `auth_tokens.py`, `auth_interceptor.py` | mTLS, tokens JWT (sesión y permisos de bloque) e interceptores de acceso |

Flujo de las operaciones (`ls /mnt/dfsha/docs`, `cat`, `cp` hacia el montaje):

```
ls  -> kernel (VFS/FUSE) -> mount.py -> client.py -> gRPC -> superpeer -> registro
cat -> mount.py -> superpeer: en qué peers está cada bloque -> peers: los bloques (varios en paralelo)
cp  -> mount.py: copia local del archivo -> al cerrarlo: superpeer (plan) -> peers (bloques) -> superpeer (commit)
```

Los bytes nunca pasan por el superpeer: solo guarda metadatos.

## Requisitos

- Linux, o Windows con WSL 2.
- Docker Engine con el plugin de Compose. Comprobar con `docker compose version`.
- FUSE disponible en el host: `ls -l /dev/fuse` debe mostrar el dispositivo.

## Primera ejecución

Desde la carpeta del proyecto (en WSL, la ruta entre comillas si tiene espacios):

```bash
# 1. Construir la imagen (la primera vez tarda unos minutos)
docker compose build

# 2. Generar el código gRPC (dfsha_pb2.py y dfsha_pb2_grpc.py).
#    No está en el repositorio: se genera a partir de dfsha.proto.
docker compose run --rm sp1 python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfsha/proto/dfsha.proto

# 3. Generar los certificados TLS: una CA propia y uno por nodo (sp1, peer1, client).
#    Toda comunicación va cifrada y con certificado en ambas puntas (mTLS). certs/ no va a git.
docker compose run --rm sp1 python -m tools.make_certs dev

# 4. Crear un usuario (pide la contraseña sin mostrarla) y darle sus credenciales al montaje.
#    --demo crea archivos de ejemplo para el usuario "demo".
docker compose run --rm sp1 python -m tools.manage_users add demo
cp .env.example .env                   # y poner en .env la contraseña elegida (.env no va a git)

# 5. Arrancar el superpeer, el peer y el cliente
docker compose up -d

# 6. Comprobar que arrancaron
docker compose ps                      # sp1, peer1, peer2, peer3 y client en estado "Up"
docker compose logs sp1                # superpeer sp1 listening on :7000 (demo data)
                                       # peer registered: peer1 at peer1:7100, ...
docker compose logs client             # connected to: sp1:7000  (o "login failed": revisar .env)
```

## Uso

El sistema de archivos queda montado en `/mnt/dfsha`, dentro del contenedor `client`:

```bash
# Un comando suelto
docker compose exec client ls -la /mnt/dfsha

# O una terminal dentro del cliente
docker compose exec client sh
cd /mnt/dfsha
mkdir -p proyectos/2026
touch notas.txt
mv notas.txt proyectos/
ls -R
rm -r proyectos
cp /app/README.md .                    # subir (en /app está la carpeta del proyecto)
head README.md                         # leer
echo "hola" > saludo.txt               # escribir; >> agrega al final
sed -i 's/hola/chao/' saludo.txt       # editar (también vi o nano, si se instalan en la imagen)
cp README.md /tmp/                     # bajar
```

Lo que se escribe se sube a los peers al cerrar el archivo: desde ahí lo ven los demás (como en NFS).
Si la subida falla, el archivo conserva su versión anterior y el error aparece al cerrar (p. ej. `cp: failed to close`).

Para detener todo: `docker compose down`. El registro del superpeer es persistente (log de operaciones +
snapshot en el volumen `sp1-data`): sobrevive a reinicios y a `down`; `docker compose down -v` lo borra.

### Usuarios

Cada usuario tiene su propio espacio de nombres: solo ve sus archivos.

```bash
docker compose run --rm sp1 python -m tools.manage_users add sofi      # crear
docker compose run --rm sp1 python -m tools.manage_users list          # listar
docker compose run --rm sp1 python -m tools.manage_users remove sofi   # borrar

# Montar como otro usuario sin editar .env:
DFSHA_USER=sofi DFSHA_PASSWORD=... docker compose up -d client
```

Los usuarios (`/data/users.json`, solo hashes de las contraseñas) y la clave que firma los tokens
(`/keys/token.key`) viven en volúmenes de `sp1`; `docker compose down -v` los borra.

### Certificados

Todos los nodos confían en la CA de `certs/`, no en una lista de nodos. Para agregar un nodo basta
con emitirle un certificado (los demás no cambian ni se reinician) y arrancarlo con `--tls-dir certs/<nombre>`:

```bash
docker compose run --rm sp1 python -m tools.make_certs issue peer peer2 --san peer2
docker compose run --rm sp1 python -m tools.make_certs issue peer peer3 --san 10.8.0.7   # en una VM: su IP de la VPN
```

El rol del certificado (`superpeer`, `peer`, `client`) limita qué puede hacer cada nodo: p. ej. solo
un `peer` puede registrarse en el superpeer y solo un `superpeer` puede dar órdenes a un peer.
Los certificados de los nodos duran 90 días y la CA un año. En una VM se copian solo
`node.key`, `node.crt` y `ca.crt` de su carpeta: `ca.key` nunca sale de donde se emiten.

Para desplegar en máquinas virtuales (CA, certificados por IP de la VPN, usuarios, renovación y
qué hacer si algo se compromete), seguir [docs/DESPLIEGUE.md](docs/DESPLIEGUE.md).

## Peers

```bash
# Las herramientas hablan con el superpeer como un cliente más (login + certificado): se ejecutan
# en el contenedor client, que tiene las credenciales del .env y el certificado de cliente
docker compose exec client python -m tools.peers_status  # tabla de peers: dirección, espacio libre, vivo o muerto
docker compose logs -f peer1                        # registro, reintentos y re-registros del peer
python3 -m tests.integration.test_peers                               # prueba automática (en WSL, ~1 min; deja sp1 y peer1 corriendo)
docker compose exec client python -m tests.integration.test_peer_blocks  # API de bloques de peer1, peer2 y peer3 (~5 s)
docker compose exec client python -m tests.integration.test_superpeer_blocks  # AllocateBlocks, CommitFile, GetBlockLocations (~2 s)
python3 -m tests.integration.test_mount                               # montaje: lectura, escritura, archivos abiertos y peers caídos (en WSL, ~30 s; deja todo corriendo)
# Respaldo sin FUSE (dfs_cli): si el montaje falla, o para ver en qué peers quedó cada bloque
docker compose exec client python -m dfsha.client.dfs_cli put /app/README.md /readme.md   # subir (en /app está la carpeta del proyecto)
docker compose exec client python -m dfsha.client.dfs_cli blocks /readme.md               # bloques y en qué peers está cada uno
docker compose exec client python -m dfsha.client.dfs_cli get /readme.md /tmp/readme.md   # bajar
```

## Superpeers

Hay dos superpeers (`sp1` y `sp2`) federados: cada usuario tiene un dueño (su *home*, o `sp1` por
defecto), el otro guarda una copia al día de su registro y lo atiende si el dueño cae.

```bash
docker compose exec client python -m tools.superpeers_status        # quién está vivo, a quién atiende cada uno
docker compose run --rm sp1 python -m tools.manage_users add luis --home sp2   # un usuario atendido por sp2
```

Al crear usuarios, escribir la contraseña en la terminal: si se canaliza desde Windows, el salto de
línea puede llegar como `CR LF` y la contraseña quedaría con un carácter invisible.

## Pruebas

```bash
# Unitarias (sin red, ~5 s): registro, persistencia, almacenamiento y cifrado, usuarios, tokens,
# limpiador y monitor de réplicas
docker compose run --rm sp1 python -m pytest tests/unit

# Integración (con todos los servicios levantados)
docker compose exec sp1 python -m tests.integration.test_security            # login, mTLS, roles, permisos de bloque (~10 s)
docker compose exec client python -m tests.integration.test_peer_blocks      # API de bloques de los peers (~5 s)
docker compose exec client python -m tests.integration.test_superpeer_blocks # plan, commit y ubicaciones (~2 s)

# Fallas (fuera de los contenedores, desde la raíz del repo): reinicia y apaga servicios
python3 -m tests.integration.test_peers      # registro, caída y regreso de un peer, reinicio del superpeer (~1 min)
python3 -m tests.integration.test_failures   # persistencia, réplica alternativa, re-replicación, limpieza (~2 min)
python3 -m tests.integration.test_mount     # montaje FUSE: cat, cp, >, >>, truncate, sed -i, archivos abiertos, peers caídos (~30 s)
python3 -m tests.integration.test_superpeers # federación: dos superpeers activos, failover con el montaje en uso, regresos (~1,5 min)
```

## Mediciones

Los scripts de `benchmarks/` miden transferencia, metadatos, costo de la seguridad y disponibilidad;
los resultados y su análisis están en [benchmarks/RESULTADOS.md](benchmarks/RESULTADOS.md).

```bash
docker compose exec client python -m benchmarks.bench_transfer     # put/get por tamaño y paralelismo (~7 min)
docker compose exec client python -m benchmarks.bench_metadata     # operaciones de metadatos por segundo
docker compose run --rm sp1 python -m benchmarks.bench_internals   # cifrado, TLS y snapshots
python3 -m benchmarks.bench_availability                           # detección, re-replicación, reinicio (~3 min)
```

## Desarrollo

| Si se modifica... | Hay que ejecutar |
|---|---|
| un archivo `.py` | `docker compose restart` (el código se monta desde la carpeta, no hace falta reconstruir) |
| `dfsha.proto` | el comando de generación del paso 2, y luego `docker compose restart` |
| `Dockerfile` | `docker compose build` y luego `docker compose up -d` |

Opciones útiles:

- `--demo` (en el comando de `sp1` en `docker-compose.yml`): el superpeer arranca con algunas carpetas y archivos de ejemplo.
- `DFSHA_DEBUG=1` (en `environment` de `client`): `mount.py` imprime cada solicitud que recibe del kernel. Para verlas: `docker compose logs -f client`.

## Problemas comunes

| Síntoma | Causa probable |
|---|---|
| `permission denied ... docker.sock` | El usuario no está en el grupo `docker`, o falta abrir una terminal nueva después de agregarlo |
| `ModuleNotFoundError: dfsha_pb2` | Falta el paso 2 (generar el código gRPC) |
| `FileNotFoundError: .../node.key` al arrancar | Falta el paso 3 (generar los certificados) |
| El cliente se reinicia con `no superpeer reachable` | `sp1` no arrancó: revisar `docker compose logs sp1` |
| `fuse: device not found` u `Operation not permitted` al montar | Faltan `devices`, `cap_add` o `security_opt` del servicio `client` en `docker-compose.yml` |
| `Transport endpoint is not connected` | `mount.py` terminó con error: `docker compose restart client` |
| `Input/output error` en cualquier comando | El superpeer no responde |
| `Input/output error` al cerrar un archivo (`cp: failed to close`) | Ningún peer pudo guardar algún bloque (p. ej. peers recién caídos); el archivo conserva su versión anterior |
| `No space left on device` al escribir | No hay peers vivos con espacio (p. ej. recién arrancados y todavía sin registrarse): esperar unos segundos y revisar `tools.peers_status` |
| `No data available` al leer | Ninguna réplica de algún bloque responde: revisar `tools.peers_status` |

## Limitaciones actuales

- Un solo superpeer: su registro se guarda en disco (log + snapshot) pero todavía no se replica a otros superpeers, y el cliente no tiene a cuál cambiarse si cae.
- Consistencia close-to-open: otro cliente ve los cambios de un archivo cuando quien lo escribe lo cierra. Si dos lo escriben a la vez, queda la versión del último que cierra.
- Los errores de subida llegan al cerrar el archivo: `cp`, Python y los editores los muestran, pero `echo ... >` de bash no revisa el cierre.
- Mientras un archivo está abierto para escribir, el cliente guarda una copia completa en su `/tmp`.
- No se guardan permisos, dueño ni fechas: `chmod`, `chown` y `touch -d` se aceptan sin efecto, y todo aparece como del usuario que montó (644 o 755).
- `df` no muestra el espacio de DFSha y `du` muestra 0.
- El montaje atiende una operación a la vez (un solo hilo).
- `rm` de un archivo abierto lo deja visible como `.fuse_hidden...` hasta que se cierra (comportamiento de libfuse).
