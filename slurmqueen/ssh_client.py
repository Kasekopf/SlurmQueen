import paramiko
from socket import gaierror
import logging

# Disable the logging of the paramiko library
logging.raiseExceptions = False


class SSHServer:
    """
    A class for interacting with a remote SSH server.
    """

    def __init__(self, server, username, key_file):
        """
        Initialize a connection to an SSH server. No network connection is actually attempted until needed.

        :param server: The address of the server
        :param username: The username to use on the server
        :param key_file: The rsa .ssh keyfile to use to login to the server
        """
        self._server = server
        self._username = username
        self._key_file = key_file
        self._client = None

    @property
    def username(self):
        return self._username

    def connection(self):
        """
        Open a connection to this server. Throws an exception if the connection fails.

        :return: A connection to this server.
        """
        if not self.is_connected():
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(self._key_file)

            try:
                self._client.connect(self._server, username=self._username, pkey=key)
            except gaierror:
                self._client = None
                raise Exception("Unable to connect to server " + self._server)

            print("Connected to " + self._server)
        return self._client

    def ftp_connect(self):
        """
        Open an sftp connection to this server. Throws an exception if the connection fails.

        :return: An sftp connection to this server.
        """
        return self.connection().open_sftp()

    def is_connected(self):
        """
        Check if we are actively connected to the server.

        :return: True if we have a connection open to the server, and False otherwise.
        """
        transport = self._client.get_transport() if self._client else None
        return transport and transport.is_active()

    def execute(self, command, other_input=None, timeout=10):
        """
        Execute a command (from the HOME directory) on this server. Throws an exception if the connection fails.

        :param command: The command to execute.
        :param other_input: Text to be written to stdin.
        :param timeout: A timeout to use when executing the command, in seconds.
        :return: All text written to stdout by the command.
        """
        stdin, stdout, stderr = self.connection().exec_command(command, timeout=timeout)

        if other_input is not None:
            stdin.write(other_input)
            stdin.flush()

        return stdout.read().decode("utf8")

    def __str__(self):
        return self._username + "@" + self._server
