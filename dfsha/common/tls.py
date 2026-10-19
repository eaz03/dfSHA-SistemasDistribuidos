"""
TLS mutuo (mTLS) en todos los canales de DFSha.

Cada proceso tiene una carpeta (tls_dir) con su identidad, creada con make_certs.py:
    node.key   su clave privada       node.crt   su certificado, firmado por la CA
    ca.crt     el certificado de la CA, para verificar a los demás

Con mTLS las dos puntas presentan certificado: el canal va cifrado, el cliente sabe que habla con
el servidor real, y el servidor sabe qué nodo le habla. Quien no tenga un certificado firmado por
la CA no llega a conectarse (la conexión se corta en el handshake, antes de cualquier RPC).

El certificado también dice el rol del nodo (superpeer, peer o client), que los servidores usan
para decidir quién puede llamar qué (ver RoleInterceptor en auth_interceptor.py).
"""

import os
from concurrent import futures

import grpc
from cryptography import x509
from cryptography.x509.oid import NameOID

# gRPC activa SO_REUSEPORT por defecto: dos procesos pueden escuchar en el MISMO puerto y el sistema
# reparte las conexiones entre ellos al azar. Con dos peers en el mismo puerto (p. ej. uno viejo que
# no terminó), eso da fallas intermitentes muy difíciles de rastrear. Apagado, el segundo falla al arrancar.
SERVER_OPTIONS = [("grpc.so_reuseport", 0)]


def _read(tls_dir, file_name):
    with open(os.path.join(tls_dir, file_name), "rb") as tls_file:
        return tls_file.read()


def create_secure_server(port, tls_dir, interceptors, max_workers=16):
    """
    Crea el servidor gRPC de un nodo, escuchando en port solo con mTLS (no hay puerto sin cifrar),
    con SERVER_OPTIONS ya aplicadas. Falta agregarle los servicios y llamar a start().
    Si el puerto ya está ocupado, termina el proceso con un mensaje claro en vez de compartirlo.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                         interceptors=interceptors, options=SERVER_OPTIONS)
    try:
        server.add_secure_port(f"[::]:{port}", server_credentials(tls_dir))
    except RuntimeError:
        raise SystemExit(f"port {port} is already in use: is another node (or an old one) still running?")
    return server


def server_credentials(tls_dir):
    """Para server.add_secure_port: presenta el certificado del nodo y exige uno válido a quien se conecte."""
    return grpc.ssl_server_credentials(
        [(_read(tls_dir, "node.key"), _read(tls_dir, "node.crt"))],
        root_certificates=_read(tls_dir, "ca.crt"),
        require_client_auth=True,          # la "m" de mTLS: sin certificado del cliente no hay conexión
    )


def secure_channel(address, tls_dir):
    """
    Reemplaza a grpc.insecure_channel(address): canal cifrado que verifica que el servidor tenga un
    certificado de la CA para el nombre de address (p. ej. "sp1"), y presenta el certificado propio.
    """
    credentials = grpc.ssl_channel_credentials(
        root_certificates=_read(tls_dir, "ca.crt"),
        private_key=_read(tls_dir, "node.key"),
        certificate_chain=_read(tls_dir, "node.crt"),
    )
    return grpc.secure_channel(address, credentials)


def caller_role(context):
    """
    Rol de quien hace la llamada ("superpeer", "peer" o "client"), leído de su certificado (campo OU),
    o None si no se puede saber. Es confiable porque el certificado ya fue verificado contra la CA
    en el handshake: nadie puede presentar uno con otro rol sin la clave de la CA.
    """
    pem_certificates = context.auth_context().get("x509_pem_cert")
    if not pem_certificates:
        return None
    certificate = x509.load_pem_x509_certificate(pem_certificates[0])
    roles = certificate.subject.get_attributes_for_oid(NameOID.ORGANIZATIONAL_UNIT_NAME)
    return roles[0].value if roles else None
