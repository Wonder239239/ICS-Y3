from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import Host, OVSKernelSwitch
from mininet.log import setLogLevel, info
from mininet.node import RemoteController
from mininet.term import makeTerm

def myTopo():
    net = Mininet(topo=None, autoSetMacs=False, build=False, ipBase='10.0.1.0/24')

    # add controller
    c1 = net.addController('c1', controller=RemoteController)

    # add hosts
    client = net.addHost('client', ip='10.0.1.5/24', mac='00:00:00:00:00:03', cls=Host, defaultRoute=None)
    server1 = net.addHost('server1', ip='10.0.1.2/24', mac='00:00:00:00:00:01', cls=Host, defaultRoute=None)
    server2 = net.addHost('server2', ip='10.0.1.3/24', mac='00:00:00:00:00:02', cls=Host, defaultRoute=None)
    
    # add switch
    s1 = net.addSwitch('s1', cls=OVSKernelSwitch, failMode='standalone')

    # add links
    net.addLink(client, s1)
    net.addLink(server1, s1)
    net.addLink(server2, s1)

    net.build()


    # start network
    net.start()

    net.terms += [makeTerm(s1), makeTerm(server1), makeTerm(server2), makeTerm(client)]

    # run CLI
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    myTopo()