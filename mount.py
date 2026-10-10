"""
 Acceso al sistema de archivos distribuidos de manera transparente para el usuario,
 montando un filesystem FUSE que se comunica con los superpeers por gRPC meadiante DFSClient.
"""

import sys
import tempfile

from fuse import FUSE, Operations, FuseOSError

from client import DFSClient

class DFShaFS(Operations):
    """Genera transparencia de accesov convirtiendo requests al kernel en llamadas al cliente DFSClient"""

    def __init__(self, client):
        self.client = client
        self.fd = 0
        self.temp_files = {}

def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python mount.py <superpeer host:port> <mountpoint>")

    client = DFSClient([sys.argv[1]])
    print(f"connected to: {client.addr}")
    FUSE(DFShaFS(client), sys.argv[2], foreground=True, nothreads=True)

if __name__ == "__main__":
    main()