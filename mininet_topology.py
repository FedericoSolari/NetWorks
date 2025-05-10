from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
import sys
import time

class CustomTopo(Topo):
    def __init__(self, num_hosts=2, packet_loss=10, link_delay=20):
        self.num_hosts   = num_hosts
        self.packet_loss = packet_loss
        self.delay       = f"{link_delay}ms"
        super().__init__()
        self.create_topology()

    def create_topology(self):
        switch = self.addSwitch("s1")

        # Add server node
        server = self.addHost("h1")
        self.addLink(server, switch,
                     cls=TCLink,
                     loss=self.packet_loss,
                     delay=self.delay)

        # Add client nodes
        for idx in range(2, self.num_hosts + 1):
            client = self.addHost(f"h{idx}")
            self.addLink(client, switch)

topos = {"CustomTopo": (lambda: CustomTopo())}

def simulate_network(host_count, server_cmd, client_cmd, pkt_loss, link_delay):
    # Construye y arranca la red
    topo = CustomTopo(
        num_hosts=host_count,
        packet_loss=pkt_loss,
        link_delay=link_delay
    )
    net = Mininet(topo=topo, link=TCLink)
    net.start()
    info(f"*** Initialized network with {host_count} hosts\n")

    # Muestra conexiones y haz ping para verificar topología
    info("*** Dumping host connections:\n")
    dumpNodeConnections(net.hosts)
    info("*** Testing connectivity (pingAll):\n")
    loss = net.pingAll()
    info(f"*** pingAll reported {loss}% packet loss\n")

    # Lanza el servidor en background y muestra sus logs
    server = net.get("h1")
    info('*** Launching server on h1 (STDOUT/ERR follows) ***\n')
    server_proc = server.popen(
        server_cmd,
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    # Esperamos un poco a que el servidor inicie
    time.sleep(2)

    # Ejecuta comando en cada cliente
    for i in range(2, host_count + 1):
        client = net.get(f"h{i}")
        info(f"*** Running client h{i}:\n")
        result = client.cmd(client_cmd)
        print(f"Output from h{i}:\n{result.strip()}\n")

    # Abre CLI interactiva para pruebas manuales
    info("*** Entering Mininet CLI — prueba manualmente, luego escribe 'exit' ***\n")
    CLI(net)

    # Al salir de la CLI, termina el servidor y detén la red
    info("*** Stopping server and shutting down network ***\n")
    server_proc.terminate()
    net.stop()

if __name__ == "__main__":
    # Subimos el nivel de logging a DEBUG para ver TODO
    setLogLevel("debug")

    if len(sys.argv) < 6:
        print(
            "Usage:\n"
            "  sudo python3 mininet_topology.py <hosts> "
            "<server_cmd> <client_cmd> <loss%> <delay_ms>\n"
        )
        sys.exit(1)

    h = int(sys.argv[1])
    s_cmd = sys.argv[2]
    c_cmd = sys.argv[3]
    loss_pct = int(sys.argv[4])
    delay_ms = int(sys.argv[5])

    simulate_network(h, s_cmd, c_cmd, loss_pct, delay_ms)
