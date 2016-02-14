from PiCom.Clients.LAN_Client import LANClientHandler, Client

__version__ = '1.6'
__author__ = "Dylan Coss <dylancoss1@gmail.com>"


class LANClient(Client):
    lan_c = None

    def __init__(self, host: str, port: int, handler: LANClientHandler = None, timeout=None, ignore_errors=False):
        print("New Client %s:%s [%s] %s" %
              (host, port, timeout if timeout is not None else "Infinite",
              ("Ignoring Errors" if ignore_errors else "")))
        super().__init__(host, port, handler, timeout, ignore_errors)
