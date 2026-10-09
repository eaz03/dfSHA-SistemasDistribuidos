
import os

import grpc, posixpath
from dfsha.proto import dfsha_pb2 as pb, dfsha_pb2_grpc as rpc


class Client:
    def __init__(self, superpeers):
        self.superpeers = superpeers
        self.token = None
        self._connect_to_superpeer()

    def _connect_to_superpeer(self):
        for superpeer in self.superpeers:
            try:
                # CONNECT_TO_SUPERPEER
                print(f"Connecting to superpeer {superpeer}...")
                return

            except Exception as e:
                print(f"Failed to connect to superpeer {superpeer}: {e}")
        return ConnectionError("no superpeer reachable")

    def login(self, username, password):
        # LOGIN
        print(f"Logging in as {username}...")
        self.token = "dummy_token"  # Replace with actual token retrieval logic
        return self.token

    def ls(self, flags, path):
        # LIST_FILES
        print(f"Listing files in {path} with flags {flags}...")
        return ["file1.txt", "file2.txt"]  # Replace with actual file listing logic

    def mkdir(self, path, dirname):
        # CREATE_DIRECTORY
        print(f"Creating directory {dirname} in {path}...")
        return True  # Replace with actual directory creation logic

    def 