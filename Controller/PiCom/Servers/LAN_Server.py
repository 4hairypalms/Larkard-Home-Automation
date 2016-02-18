import socket
import threading

from PiCom.Clients import LANClient
from PiCom.Data import print_payload, Payload, build_payload, send_payload, \
    PayloadEventMessages
from PiCom.Data.Structure import PayloadType, PayloadEvent, BLANK_FIELD, WILDCARD, EventTypes, EventDomain
from PiCom.Delegation import PiDiscovery
from PiCom.Servers.SystemControllerUtils import Responder, SYS_Handler

"""
    :SERVER
        This is the multi threaded server for handling all communications to the units on
        the local network. For every new client that connects, a new thread is spawned.

    :PAYLOAD PROCESSING
        As the payloads are being received, their 'role' parameter is being compared
        to that of the current servers 'role', if the role is'nt intended for the
        receiving server, the payload will be ignored and a 'NotIntended' event will
        be replied. However, if delegation is enabled. Then the server will ID the
        correct server and relay both the request and response back to the client
        from the intended source.

        If the payload is intended for the current server, the payload is interpreted.
        If however its a instruction for the hardware, its sent to the handler.

    :HANDLER
        On creation of the server, a handler needs to be set, the server will fire events
        to that handler as they are received, the callback from the handler will feature as
        the response to the client (APP/WEB).

            |===PI===| ----->----->----- *+++++++++*     * ============== *
        <** | SERVER |     CALLBACK      | HANDLER |-<  >- IMPLEMENTATION *
            |________| -----<-----<----- *++++++++++     * ============== *



"""
__version__ = '1.5'
__author__ = "Dylan Coss <dylancoss1@gmail.com>"

IS_DELEGATOR = True
ROLE = None
NAME = None
REQUEST_TYPES = EventTypes.REQUEST_TYPES.value
HARDWARE_TYPES = EventTypes.HARDWARE_EVENTS.value
SOFTWARE_TYPES = EventTypes.SOFTWARE_EVENT.value
SYSTEM_TYPES = EventTypes.SYSTEM_EVENTS.value


class Client(threading.Thread, Responder):
    def __init__(self, ip: str, port: int, client_soc: socket, handler: SYS_Handler,
                 delegation_event_whitelist=None):
        assert handler is not None
        threading.Thread.__init__(self)
        self.lock = threading.Lock()
        if delegation_event_whitelist is None:
            delegation_event_whitelist = []
        self.client_address = ip
        self.port = port
        self.request = client_soc
        self.handler = handler
        self.del_whitelist = delegation_event_whitelist
        print("[+] Connection to {} Established: ".format(self.client_address))

    def data_handler(self, payload: Payload, delegation_event_whitelist=None):
        if delegation_event_whitelist is None:
            delegation_event_whitelist = []
        notify_all = payload.role == WILDCARD
        correct_payload = payload.role == ROLE
        is_request = payload.type in REQUEST_TYPES
        white_listed = payload.event in delegation_event_whitelist

        # 1. Determines if the payload is intended for the current unit
        if not correct_payload and is_request and not white_listed:

            # 2 If the current payload is NOT intended for the current unit
            #   it will be delegated, and hopefully send it to the appropriate
            #   unit.
            if IS_DELEGATOR and payload.role is not BLANK_FIELD:
                # 2.1 If delegation is enabled, it then gets the ip address of the intended
                #     unit and sends the payload, if the wildcard has been specified then all
                #     the units will be notified

                addresses = PiDiscovery.get_ip_addresses(payload.role)
                transactions = []
                res_payload = PayloadEventMessages.ADDRESS_NOT_FOUND.value
                for address in addresses:
                    client = LANClient(address, self.port, timeout=5, ignore_errors=True)
                    print("\t%s\n\t|\t\tRelaying -> %s:%s" % (print_payload(payload), address, self.port))
                    res_payload = client.send(payload)
                    transactions.append((res_payload.data, res_payload.event.name))

                if len(transactions) > 1:
                    payload.data = transactions
                    payload.type = PayloadType.RSP
                    return payload
                return res_payload

            else:
                # 2.2 If delegation is disabled, and the role can't be handled, then
                #     a error response will be sent telling the client 'Wrong Node'.
                if notify_all:
                    return self.process_payload(payload)
                return PayloadEventMessages.WRONG_NODE

        return self.process_payload(payload)

    def process_payload(self, payload: Payload):
        res_payload = payload
        is_resp = payload.type not in REQUEST_TYPES
        is_soft = payload.event in SOFTWARE_TYPES
        is_hard = payload.event in HARDWARE_TYPES
        is_sys = payload.event in SYSTEM_TYPES

        print("[i] Processing %s %s (%s)" %
              ("Hardware" if is_hard else ("Software" if is_soft else ("System" if is_sys else "Unknown")),
               "Response" if is_resp else "Request",
               print_payload(payload)))

        # --------------------SYSTEM HANDLING--------------------
        if is_sys:
            if is_resp:
                # Response handling
                # TODO: Handle response
                pass

            else:
                # Request handling
                # Handles Probe Request
                if payload.event is PayloadEvent.S_PROBE:
                    res_payload.data = {'name': NAME, 'role': ROLE, 'isDelegator': IS_DELEGATOR}
                    res_payload.type = PayloadType.RSP
                # TODO: Handle requests

                    # ------------------HARDWARE/SOFTWARE HANDLING------------------------
        if is_hard or is_soft:
            domain = EventDomain.SOFT if is_soft else (EventDomain.GPIO if is_hard else None)
            print("\t%s Instruction <%s --> %s>" % (domain.value, payload.data, payload.event))
            return self.handler.instruction(self, payload.data, payload.event, domain, payload)
        return res_payload

    def send_event_message(self, E: PayloadEventMessages, message: str = None):
        E.value.data = {'message': message}
        send_payload(self.request, E)

    def respond(self, payload: Payload):
        if payload is None:
            send_payload(self.request, PayloadEventMessages.ADDRESS_NOT_FOUND)

        else:
            send_payload(self.request, payload)

    def run(self):
        res_data = None
        running = True
        self.lock.acquire()
        while running:
            try:
                req_data = self.request.recv(1024)
                if req_data is not None and (len(req_data) > 0):
                    req_data = build_payload(req_data.decode("utf8"))
                    if isinstance(req_data, Payload):
                        # Sends the received data to be manage by a helper function
                        res_data = self.data_handler(req_data, self.del_whitelist)
                        if running:
                            self.respond(res_data)
                    else:
                        print("[!] Raw data retrieved: {}".format(res_data))
                else:
                    self.send_event_message(PayloadEventMessages.ERROR,
                                            "There was no data received!")
            except socket.error as err:
                if err.errno not in [104, 32]:
                    raise
                running = False
        print("[-] Connection to {} Closed \n".format(self.client_address))


def start_lan_server(handler: SYS_Handler, ip_address="0.0.0.0", port=8000, execution_role="No Role",
                     name="Unknown", delegation_event_whitelist: list = None, delegator=True):
    global ROLE, NAME, IS_DELEGATOR
    ROLE = execution_role
    NAME = name
    IS_DELEGATOR = delegator

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_socket.bind((ip_address, port))

    print("\n[!] %s %s (%s)\n\tListening on %s:%d\n" %
          (NAME, ("Delegator" if IS_DELEGATOR else "Node"), ROLE, ip_address, port))

    client_threads = []

    while True:
        tcp_socket.listen(10)
        (client_socket, client_ip) = tcp_socket.accept()
        spawned_thread = Client(client_ip, port, client_socket, handler, delegation_event_whitelist)
        spawned_thread.start()
        client_threads.append(spawned_thread)
        client_threads = [t for t in client_threads if not t.isAlive()]
        # Adds to the thread pool
    for thread in client_threads:
        thread.join()
