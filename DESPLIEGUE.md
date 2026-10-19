# DFSha: despliegue en máquinas virtuales (seguridad)

Guía para preparar la seguridad de DFSha en VMs (AWS Academy / GCP) conectadas por VPN:
certificados TLS, usuarios y claves. Para correrlo en local con Docker Compose, ver el README.

> Estado: la configuración de las VMs y de la VPN (fase 6) todavía no está definida. Esta guía
> cubre lo que no depende de esas decisiones; los comandos de arranque son un ejemplo a ajustar.

## 1. Cómo funciona la seguridad (resumen)

| Pieza | Qué protege | Dónde vive |
|---|---|---|
| **CA propia** (`ca.key` + `ca.crt`) | Firma los certificados de todos los nodos. Todos confían en ella, no en una lista de nodos | `ca.key`: **solo** en la máquina del administrador. `ca.crt`: en todos los nodos |
| **Certificado de cada nodo** (`node.key` + `node.crt`) | mTLS: canal cifrado y cada punta prueba quién es. El campo OU dice el rol (`superpeer`, `peer`, `client`) | En el nodo, en `certs/<nombre>/` |
| **Usuarios** (`users.json`) | Login con usuario/contraseña (se guardan hashes scrypt) | En el superpeer, `/data/users.json` |
| **Clave de tokens** (`token.key`) | Firma los tokens de sesión (JWT EdDSA) que se entregan después del login | En el superpeer, `/keys/token.key`; se crea sola al arrancar |
| **Clave de bloques** | Cifrado en reposo de los bloques (AES-256-GCM) | En cada peer, en un lugar distinto de los bloques (p. ej. `/keys/blocks.key`) |

Reglas por rol, que aplican los servidores leyendo el certificado de quien llama:
- Solo un `peer` puede registrarse en el superpeer (`RegisterPeer`, `Heartbeat`).
- Solo un `superpeer` puede dar órdenes a un peer (`Ping`, y después `DeleteBlock`).
- Las operaciones de archivos exigen además un token de usuario válido.

Sin un certificado firmado por la CA, una máquina **no llega a conectarse**: la conexión se corta
en el handshake, antes de cualquier RPC.

## 2. Antes de empezar

- [ ] **IPs de la VPN** de cada VM decididas (p. ej. superpeer `10.8.0.1`, peers `10.8.0.5`, `10.8.0.6`...).
      Los certificados se emiten para esas IPs: si cambian, hay que reemitirlos.
- [ ] **Puertos abiertos dentro de la VPN** (grupos de seguridad / firewall): `7000` (superpeer) y `7100` (peers).
      No hace falta exponerlos a Internet: todo va por la VPN.
- [ ] **Relojes sincronizados** en todas las VMs (NTP: `timedatectl set-ntp true`). Los tokens toleran
      30 s de diferencia y los certificados tienen fecha de inicio y vencimiento: un reloj muy
      desfasado hace fallar el login o el handshake.
- [ ] Una **máquina de administración** (p. ej. el computador de quien despliega) con el repositorio
      y Docker, o Python con el paquete `cryptography`. Ahí vive la CA.

## 3. Crear la CA (una sola vez en todo el proyecto)

En la máquina de administración, desde la carpeta del repositorio:

```bash
docker compose run --rm sp1 python make_certs.py init
# o, con Python local: python make_certs.py init
```

Crea `certs/ca.key` y `certs/ca.crt` (válida 1 año).

- **Haz una copia de seguridad de `certs/ca.key` en un lugar seguro y fuera del repositorio.**
  Si se pierde, no se pueden emitir certificados para nodos nuevos y hay que regenerar todo (sección 8).
- **`ca.key` nunca se copia a una VM.** Quien la tenga puede fabricar el certificado de cualquier nodo,
  incluido uno de superpeer.
- `make_certs.py init` se niega a correr si ya existe una CA: regenerarla invalidaría todos los certificados.

## 4. Emitir el certificado de cada nodo

Uno por nodo, con su nombre y **la IP con la que los demás se conectan a él** (`--san`).
TLS comprueba que la dirección a la que uno se conecta esté en el certificado del otro.

```bash
python make_certs.py issue superpeer sp1   --san 10.8.0.1
python make_certs.py issue peer      peer1 --san 10.8.0.5
python make_certs.py issue peer      peer2 --san 10.8.0.6
python make_certs.py issue client    cliente-sofi           # una por máquina cliente
```

(Con Docker: anteponer `docker compose run --rm sp1` a cada comando.)

Cada uno queda en `certs/<nombre>/` con tres archivos: `node.key`, `node.crt` y `ca.crt`.
Los certificados de nodo duran **90 días** (ver sección 7).

- Si un nodo tiene **varios nombres** (IP de la VPN y un nombre DNS), se repite `--san`:
  `--san 10.8.0.5 --san peer1.dfsha.internal`.
- Los clientes no necesitan `--san`: nadie se conecta a ellos.

## 5. Llevar los archivos a cada VM

Se copia **solo la carpeta de ese nodo** (sus tres archivos), nunca `certs/` completa:

```bash
scp -r certs/peer1 ubuntu@10.8.0.5:~/dfsha/certs/
ssh ubuntu@10.8.0.5 "chmod 600 ~/dfsha/certs/peer1/node.key"
```

Comprobar que en la VM quedó exactamente esto y nada más:

```
~/dfsha/certs/peer1/node.key   (permisos 600)
~/dfsha/certs/peer1/node.crt
~/dfsha/certs/peer1/ca.crt
```

## 6. Arrancar cada nodo

Ejemplo con los programas directamente (si se usa Docker en la VM, son los mismos argumentos en
el `command` del servicio, y los volúmenes `/data` y `/keys` deben ser persistentes):

**Superpeer** (`10.8.0.1`):
```bash
python superpeer.py --id sp1 --port 7000 --tls-dir certs/sp1 \
    --users-file /data/users.json --token-key /keys/token.key
```
Crear los usuarios en esa misma VM (pide la contraseña sin mostrarla):
```bash
python manage_users.py --file /data/users.json add sofi
```

**Peer** (`10.8.0.5`), con su propia IP en `--host` (es la que anuncia al superpeer y la que está en su certificado):
```bash
python peer.py --id peer1 --host 10.8.0.5 --port 7100 --superpeer 10.8.0.1:7000 \
    --capacity-mb 1024 --data-dir /data --tls-dir certs/peer1
```

**Cliente** (montaje FUSE):
```bash
export DFSHA_USER=sofi DFSHA_PASSWORD='...' DFSHA_TLS_DIR=certs/cliente-sofi
python mount.py 10.8.0.1:7000 /mnt/dfsha
```

Comprobar:
- en el log del superpeer: `peer registered: peer1 at 10.8.0.5:7100`;
- en el cliente: `connected to: 10.8.0.1:7000`, y `ls /mnt/dfsha` responde.

## 7. Agregar un nodo, renovar y revisar vencimientos

**Agregar un nodo:** secciones 4, 5 y 6 solo para ese nodo. Los demás no cambian ni se reinician:
confían en la CA, no en una lista de nodos.

**Ver cuándo vence un certificado:**
```bash
openssl x509 -in certs/peer1/node.crt -noout -subject -enddate
```

**Renovar** (antes de los 90 días; el proyecto dura menos, pero por si acaso):
```bash
python make_certs.py issue peer peer1 --san 10.8.0.5 --force
```
y repetir las secciones 5 y 6 (copiar y reiniciar ese nodo).

**Si cambia la IP de un nodo:** renovar con la IP nueva en `--san` (mismo comando con `--force`).

## 8. Si algo se compromete

| Qué se filtró | Qué hacer |
|---|---|
| La clave de **un nodo** (`node.key`) | No hay revocación: ese certificado sirve hasta que venza (máx. 90 días). Para cortarlo ya, regenerar la CA y reemitir todos los certificados (abajo). Mientras tanto, sacar la máquina de la VPN |
| **`ca.key`** | Regenerar la CA y reemitir todos los certificados (abajo). Es lo más grave: quien la tenga puede hacerse pasar por cualquier nodo |
| **`token.key`** del superpeer | Borrarla y reiniciar el superpeer: se crea una nueva e invalida todos los tokens; los clientes vuelven a hacer login solos |
| La contraseña de un usuario | `manage_users.py remove <usuario>` y `add <usuario>` con una nueva. Sus tokens actuales siguen sirviendo hasta 1 h |

**Regenerar la CA:** mover la carpeta `certs/` vieja a otro lado, `make_certs.py init`, emitir de
nuevo los certificados de todos los nodos (sección 4), copiarlos (5) y reiniciar todo (6).

## 9. Problemas comunes

| Síntoma | Causa probable |
|---|---|
| `FileNotFoundError: .../node.key` al arrancar | La carpeta de `--tls-dir` no existe en esa VM o le faltan archivos (sección 5) |
| `UNAVAILABLE: failed to connect` y en el log `Handshake failed` | Certificado de otra CA, vencido, o sin certificado. Revisar que `ca.crt` sea el mismo en ambos nodos y la fecha con `openssl x509 -enddate` |
| Handshake falla solo hacia un nodo | La dirección usada no está en su certificado: p. ej. el peer anuncia `10.8.0.5` pero su certificado se emitió con otra IP. Reemitir con el `--san` correcto |
| `PERMISSION_DENIED: only a peer can call this method` | El nodo arrancó con un certificado de otro rol (p. ej. el de un cliente). Revisar `--tls-dir` |
| `login failed: invalid username or password` | El usuario no existe en `users.json` de ese superpeer, o la contraseña es otra |
| `token expired` o fallos de handshake intermitentes | Relojes desincronizados entre VMs (sección 2) |

## 10. Mejora futura: inscripción automática

Hoy un administrador emite y copia los certificados a mano (una vez por nodo). La alternativa,
descartada por ahora por su costo, es la **inscripción automática** (como `kubeadm join` o `step-ca`):

1. El administrador genera un **token de inscripción de un solo uso** para el nodo nuevo.
2. El nodo arranca con ese token, **genera su clave en su propia máquina** (la clave nunca viaja),
   pide al superpeer que le firme el certificado y verifica al superpeer con la huella de la CA.
3. El superpeer firma con el rol y nombre que dice el token (no los que pida el nodo).
4. El nodo **renueva solo** antes de que venza, con su certificado actual. Eso permite certificados
   de horas en vez de días, lo que casi elimina la necesidad de revocación; expulsar un nodo es
   dejar de renovarle.

Requiere un puerto de inscripción aparte (sin mTLS, solo para inscribirse) y que la CA firme en
línea; lo recomendable sería una CA intermedia por superpeer bajo una raíz guardada fuera de
línea, que además encaja con superpeers de organizaciones distintas. Antes de construirlo,
evaluar usar `step-ca` en vez de implementarlo a mano.
