# DFSha

Sistema de archivos distribuido (DFS) basado en bloques, organizado como una red P2P con superpeers.
Proyecto 1 de Sistemas Distribuidos (SI3007), Universidad EAFIT, 2026-2.

> **Estado: preliminar.** Funciona el espacio de nombres completo a través de un montaje FUSE:
> `ls`, `cd`, `stat`, `find`, `mkdir`, `rmdir`, `rm`, `mv`, `touch`.
> La lectura y escritura del contenido de los archivos (bloques en los peers) está en desarrollo.

## Componentes

| Archivo | Rol |
|---|---|
| `mount.py` | Cliente. Monta DFSha como una carpeta común (FUSE): cualquier programa (`ls`, `cd`, `mkdir`...) la usa sin saber que es remota |
| `client.py` | `DFSClient`: envía las operaciones al superpeer por gRPC |
| `superpeer.py` | Servidor gRPC del superpeer: recibe cada operación y la aplica al registro de metadatos |
| `registry.py` | Contrato (interfaz) del registro de metadatos |
| `dict_registry.py` | Implementación actual del registro: diccionarios en memoria |
| `errors.py` | Errores de sistema de archivos y su traducción entre capas (registro, red, kernel) |
| `dfsha.proto` | Contrato de comunicación gRPC entre cliente y superpeer |
| `Dockerfile`, `docker-compose.yml` | Imagen y contenedores: `sp1` (superpeer) y `client` (montaje) |

Flujo de una operación, por ejemplo `ls /mnt/dfsha/docs`:

```
ls -> kernel (VFS/FUSE) -> mount.py -> client.py -> gRPC -> superpeer.py -> registro
```

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
docker compose run --rm sp1 python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfsha.proto

# 3. Generar los certificados TLS: una CA propia y uno por nodo (sp1, peer1, client).
#    Toda comunicación va cifrada y con certificado en ambas puntas (mTLS). certs/ no va a git.
docker compose run --rm sp1 python make_certs.py dev

# 4. Crear un usuario (pide la contraseña sin mostrarla) y darle sus credenciales al montaje.
#    --demo crea archivos de ejemplo para el usuario "demo".
docker compose run --rm sp1 python manage_users.py add demo
cp .env.example .env                   # y poner en .env la contraseña elegida (.env no va a git)

# 5. Arrancar el superpeer, el peer y el cliente
docker compose up -d

# 6. Comprobar que arrancaron
docker compose ps                      # sp1 y client en estado "Up"
docker compose logs sp1                # superpeer sp1 listening on :7000 (demo data)
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
```

Para detener todo: `docker compose down`. El registro vive en memoria, así que se pierde al detener el superpeer.

### Usuarios

Cada usuario tiene su propio espacio de nombres: solo ve sus archivos.

```bash
docker compose run --rm sp1 python manage_users.py add sofi      # crear
docker compose run --rm sp1 python manage_users.py list          # listar
docker compose run --rm sp1 python manage_users.py remove sofi   # borrar

# Montar como otro usuario sin editar .env:
DFSHA_USER=sofi DFSHA_PASSWORD=... docker compose up -d client
```

Los usuarios (`/data/users.json`, solo hashes de las contraseñas) y la clave que firma los tokens
(`/keys/token.key`) viven en volúmenes de `sp1`; `docker compose down -v` los borra.

### Certificados

Todos los nodos confían en la CA de `certs/`, no en una lista de nodos. Para agregar un nodo basta
con emitirle un certificado (los demás no cambian ni se reinician) y arrancarlo con `--tls-dir certs/<nombre>`:

```bash
docker compose run --rm sp1 python make_certs.py issue peer peer2 --san peer2
docker compose run --rm sp1 python make_certs.py issue peer peer3 --san 10.8.0.7   # en una VM: su IP de la VPN
```

El rol del certificado (`superpeer`, `peer`, `client`) limita qué puede hacer cada nodo: p. ej. solo
un `peer` puede registrarse en el superpeer y solo un `superpeer` puede dar órdenes a un peer.
Los certificados de los nodos duran 90 días y la CA un año. En una VM se copian solo
`node.key`, `node.crt` y `ca.crt` de su carpeta: `ca.key` nunca sale de donde se emiten.

Para desplegar en máquinas virtuales (CA, certificados por IP de la VPN, usuarios, renovación y
qué hacer si algo se compromete), seguir [DESPLIEGUE.md](DESPLIEGUE.md).

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
| `Input/output error` en cualquier comando | El superpeer no responde, o la operación todavía no está implementada (ver limitaciones) |

## Limitaciones actuales

- Todavía no se guarda contenido: `cat`, `cp` y escribir en un archivo fallan. Los archivos existen solo como entradas del registro, con tamaño 0.
- Un solo superpeer. Hay login con usuario/contraseña, tokens y mTLS en todos los canales; el cifrado de los bloques en disco está listo (`EncryptedBlockStorage`) pero el peer todavía no guarda bloques.
- El registro está en memoria: se reinicia junto con el superpeer.
