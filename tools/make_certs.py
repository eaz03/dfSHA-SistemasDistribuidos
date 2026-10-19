"""
Certificados de DFSha: una autoridad certificadora (CA) propia y un certificado por nodo.

Todos los nodos confían en la CA, no en una lista de nodos: un peer nuevo solo necesita que se le
emita un certificado, sin tocar ni reiniciar a los demás.

    python -m tools.make_certs init                                # una sola vez: crea la CA
    python -m tools.make_certs issue peer peer2 --san peer2        # un nodo nuevo
    python -m tools.make_certs issue peer peer3 --san 10.8.0.7     # en una VM, con su IP de la VPN
    python -m tools.make_certs dev                                 # CA + los nodos del docker-compose

Cada nodo queda en certs/<nombre>/ con node.key, node.crt y ca.crt: es lo único que hay que
copiarle. ca.key (la clave de la CA) nunca sale de la máquina donde se emiten los certificados:
quien la tenga puede fabricar el certificado de cualquier nodo.
"""

import argparse
import datetime
import ipaddress
import os
import sys

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from dfsha.common.key_files import create_file_exclusively

CERTS_DIRECTORY = "certs"
ROLES = ("superpeer", "peer", "client")
# Los certificados de los nodos duran 90 días (el estándar actual, p. ej. Let's Encrypt): si una
# clave se filtra, sirve poco tiempo. La CA dura más porque ningún certificado puede vencer después
# que quien lo firmó: un nodo creado el día 60 tendría un certificado de solo 30 días.
NODE_VALIDITY_DAYS = 90
CA_VALIDITY_DAYS = 365
# Los nodos del docker-compose y los nombres con que se los encuentra (localhost: para pruebas
# dentro del mismo contenedor)
DEV_NODES = [("superpeer", "sp1", ["sp1", "localhost"]),
             ("peer", "peer1", ["peer1", "localhost"]),
             ("peer", "peer2", ["peer2"]),
             ("peer", "peer3", ["peer3"]),
             ("client", "client", ["client"])]


# ---------- Archivos ----------

def _key_pem(private_key):
    return private_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption())


def _certificate_pem(certificate):
    return certificate.public_bytes(serialization.Encoding.PEM)


def _write(path, content, mode, overwrite):
    """Escribe un archivo nuevo; si ya existe, solo lo reemplaza con overwrite (si no, devuelve False)."""
    if overwrite and os.path.exists(path):
        os.remove(path)
    return create_file_exclusively(path, content, mode)


def _load_ca(directory):
    with open(os.path.join(directory, "ca.key"), "rb") as key_file:
        ca_key = serialization.load_pem_private_key(key_file.read(), password=None)
    with open(os.path.join(directory, "ca.crt"), "rb") as certificate_file:
        ca_certificate = x509.load_pem_x509_certificate(certificate_file.read())
    return ca_key, ca_certificate


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


# ---------- CA ----------

def create_ca(directory):
    """Crea la CA (ca.key + ca.crt). Se niega si ya existe: regenerarla invalidaría todos los certificados."""
    if os.path.exists(os.path.join(directory, "ca.key")):
        raise SystemExit("error: the CA already exists; recreating it would invalidate every node certificate")

    ca_key = ec.generate_private_key(ec.SECP256R1())     # curva P-256: estándar y soportada por TLS en todas partes
    name = x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DFSha"),
                      x509.NameAttribute(NameOID.COMMON_NAME, "DFSha CA")])
    now = _now()
    ca_certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)                               # se firma a sí misma: es la raíz de la confianza
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))   # margen por relojes desfasados
        .not_valid_after(now + datetime.timedelta(days=CA_VALIDITY_DAYS))
        # ca=True: puede firmar otros certificados; path_length=0: los que firme no pueden firmar a su vez
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False, key_encipherment=False,
                                     data_encipherment=False, key_agreement=False, key_cert_sign=True,
                                     crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    _write(os.path.join(directory, "ca.key"), _key_pem(ca_key), 0o600, overwrite=False)
    _write(os.path.join(directory, "ca.crt"), _certificate_pem(ca_certificate), 0o644, overwrite=False)
    print(f"CA created in {directory}/ (valid {CA_VALIDITY_DAYS} days). Keep ca.key private.")


# ---------- Nodos ----------

def issue_node_certificate(directory, role, node_name, alternative_names, force):
    """
    Emite el certificado de un nodo firmado por la CA y lo deja en directory/node_name/.
    role va en el certificado (campo OU): los servidores lo leen para decidir qué puede hacer
    quien llama (p. ej. solo un "peer" puede registrarse como peer).
    alternative_names: nombres o IPs con que otros se conectan a este nodo; TLS comprueba que la
    dirección a la que uno se conecta esté aquí.
    """
    if role not in ROLES:
        raise SystemExit(f"error: role must be one of {', '.join(ROLES)}")
    node_directory = os.path.join(directory, node_name)
    if os.path.exists(os.path.join(node_directory, "node.crt")) and not force:
        raise SystemExit(f"error: {node_name} already has a certificate (use --force to replace it)")
    ca_key, ca_certificate = _load_ca(directory)

    san_entries = []
    for alternative_name in dict.fromkeys([node_name] + alternative_names):   # sin repetidos, en orden
        try:
            san_entries.append(x509.IPAddress(ipaddress.ip_address(alternative_name)))
        except ValueError:
            san_entries.append(x509.DNSName(alternative_name))

    node_key = ec.generate_private_key(ec.SECP256R1())
    now = _now()
    not_valid_after = min(now + datetime.timedelta(days=NODE_VALIDITY_DAYS), ca_certificate.not_valid_after_utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DFSha"),
                                 x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, role),
                                 x509.NameAttribute(NameOID.COMMON_NAME, node_name)]))
        .issuer_name(ca_certificate.subject)
        .public_key(node_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(not_valid_after)
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        # Sirve como servidor (otros se conectan a él) y como cliente (él se conecta a otros, con mTLS)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                              ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    _write(os.path.join(node_directory, "node.key"), _key_pem(node_key), 0o600, overwrite=True)
    _write(os.path.join(node_directory, "node.crt"), _certificate_pem(certificate), 0o644, overwrite=True)
    _write(os.path.join(node_directory, "ca.crt"), _certificate_pem(ca_certificate), 0o644, overwrite=True)
    print(f"{role} certificate for {node_name} ({', '.join(dict.fromkeys([node_name] + alternative_names))}) "
          f"in {node_directory}/, valid until {not_valid_after:%Y-%m-%d}")


def main():
    parser = argparse.ArgumentParser(description="Certificados TLS de DFSha")
    parser.add_argument("--dir", default=CERTS_DIRECTORY, help="carpeta de certificados")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("init", help="crear la CA (una sola vez)")
    issue = subcommands.add_parser("issue", help="emitir el certificado de un nodo")
    issue.add_argument("role", choices=ROLES)
    issue.add_argument("name", help="nombre del nodo, p. ej. peer2")
    issue.add_argument("--san", action="append", default=[], help="otro nombre o IP del nodo (repetible)")
    issue.add_argument("--force", action="store_true", help="reemplazar un certificado existente")
    subcommands.add_parser("dev", help="CA (si falta) + certificados de los nodos del docker-compose (si faltan)")
    arguments = parser.parse_args()

    if arguments.command == "init":
        create_ca(arguments.dir)
    elif arguments.command == "issue":
        issue_node_certificate(arguments.dir, arguments.role, arguments.name, arguments.san, arguments.force)
    elif arguments.command == "dev":
        if not os.path.exists(os.path.join(arguments.dir, "ca.key")):
            create_ca(arguments.dir)
        for role, node_name, alternative_names in DEV_NODES:
            if os.path.exists(os.path.join(arguments.dir, node_name, "node.crt")):
                print(f"{node_name}: already has a certificate")
            else:
                issue_node_certificate(arguments.dir, role, node_name, alternative_names, force=False)


if __name__ == "__main__":
    sys.exit(main())
