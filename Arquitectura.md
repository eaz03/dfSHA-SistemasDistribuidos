# DFSha: Arquitectura P2P con superpeers

##  1. Vista general

DFSha es un sistema de archivos distribuido (dfs) basado en bloques y organizado como red P2P con superpeers. Los peers almacenan los bloques correspondientes a archivos particionados. Los superpeers, adicional a almacenar información mantienen un registro de donde se encuentran los bloques, los cuales están replicados entre varios peers. Como pueden haber varios superpeers, el registro de bloques es replicado entre los superpeers.

*Cliente:*

Ejecuta los comandos de información del directorio, asi como añadir bloques, y acceder a bloques.

*Superpeer (NameNode y DataNode)*
- Registro de metadatos, localización y partición de lo archivos.
- Asignación de bloques a DataNodes
- Detección de fallas
- Sincronización de registros de metadatos

*peer (DataNode)* 

Almacena y entrega los bloques cuando es necesario usando el ID del bloque.

### Comunicación

*Cliente y Superpeer:*
Se comunican para la autenticación de usuarios, operaciones en el espacio de archivos, localización de bloques, request de acceso a bloques o envío de bloques. Utiliza gRPC.

*Superpeer y Superpeer:*
Se comunican a través de gRPC para la sincronización de registros de bloques. 

*Superpeer y peer* 
Heartbeat para saber que sigue activo. Comandos de replicación de archivos o de borrado. Se comunican a través de gRPC.

## 2. Cliente

*Conexión inicial:* Lee una lista de superpeers conocidos, intenta conectarse por gRPC y si falla sigue al siguiente.

*Autenticación:* Recibe solicitud de usuario y contraseña, DETALLAR AUTENTICACIÓN CON TOKENS.

*SI SE HACE CD LOCAL*
El superpeer recibe solo path absoluto, antes de envíar cualquier cosa, se convierte a path absoluto.

*Operaciones de espacio de nombre (namespace)*
ls, mkdir, rmdir, rm, mv, stat
El superpeer devuelve la estructura de archivos que guarda en memoria, o la modifica, sin alterar el almacenamiento subyacente en bloques.

*Operaciones sobre archivos*

put y get

## 3. Superpeer (NameNode + DataNode)

### 3.1 Inicio e incorporación a la red
Se inicia en un servidor aparte eg. ""

1. Contacta a otro superpeer en su configuración
2. Recibe la lista de superpeers y el registro de metadata actual
3. Se subscribe a las actualizaciones de registro y se anuncia en ellas

### 3.2 Contenido del registro
### 3.3 Almacenamiento del registro [ABIERTO]
### 3.4 Sincronización del registro [ABIERTO]
### 3.5 Ubicación y replicación
## 4. Peer (DataNode)
## 5. Flujos de operaciones
## 6. Seguridad [PENDIENTE]