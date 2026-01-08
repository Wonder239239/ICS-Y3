from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, in_proto
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.lib.packet import ipv4, tcp

class RyuRedirect(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    
    def __init__(self, *args, **kwargs):
        super(RyuRedirect, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.SERVER1 = {'mac': '00:00:00:00:00:01', 'ip': '10.0.1.2'}
        self.SERVER2 = {'mac': '00:00:00:00:00:02', 'ip': '10.0.1.3'}
        self.CLIENT = {'mac': '00:00:00:00:00:03', 'ip': '10.0.1.5'}
    
    # table-miss rule
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):

        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        # lowest priority
        self.add_flow(datapath, 0, match, actions, buffer_id=None)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=5):

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        # excluding table-miss rule
        timeout = idle_timeout if priority > 0 else 0

        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, 
                                    priority=priority, 
                                    match=match, 
                                    instructions=inst, 
                                    buffer_id=buffer_id,
                                    idle_timeout=timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, 
                                    priority=priority, 
                                    match=match, 
                                    instructions=inst,
                                    idle_timeout=timeout)
        datapath.send_msg(mod)
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes", ev.msg.msg_len, ev.msg.total_len)
        
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        # ignore lldp packets
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        
        # learn mac
        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD


         # get Server2 port
        if self.SERVER2['mac'] in self.mac_to_port[dpid]:
            server2_port = self.mac_to_port[dpid][self.SERVER2['mac']]
        else:
            self.logger.warning("Server2 MAC not learned yet, flood this SYN")
            server2_port = ofproto.OFPP_FLOOD


        actions = [parser.OFPActionOutput(out_port)]

  
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        protocol=ip_pkt.proto if ip_pkt else None
        #check TCP Protocol and IPV4 protocol
        if ip_pkt and protocol == in_proto.IPPROTO_TCP and tcp_pkt:
            #check TCP SYN
            if (tcp_pkt.bits & tcp.TCP_SYN) and not (tcp_pkt.bits & tcp.TCP_ACK):
                
                if ip_pkt.src == self.CLIENT['ip'] and ip_pkt.dst == self.SERVER1['ip']:
                    self.logger.info("Detected TCP SYN from Client to Server1, installing REDIRECT flows to Server2")

                client_port = in_port

                #Client -> Server1 (original dst) redirected to Server2
                actions_client_to_server2 = [
                        parser.OFPActionSetField(eth_dst=self.SERVER2['mac']),
                        parser.OFPActionSetField(ipv4_dst=self.SERVER2['ip']),
                        parser.OFPActionOutput(server2_port),
                    ]
                # Only install flows if we know Server2's port
                if server2_port != ofproto.OFPP_FLOOD:
                    # Flow 1: Client -> Server1 (original dst) redirected to Server2
                    match_client_to_server2 = parser.OFPMatch(
                        in_port=client_port,
                        eth_type=ether_types.ETH_TYPE_IP,
                        ip_proto=in_proto.IPPROTO_TCP,
                        ipv4_src=self.CLIENT['ip'],
                        ipv4_dst=self.SERVER1['ip'],
                    )
                    self.add_flow(datapath, 1, match_client_to_server2, actions_client_to_server2)

                    # Flow 2: Server2 -> Client masquerade as Server1
                    match_server2_to_client = parser.OFPMatch(
                        in_port=server2_port,
                        eth_type=ether_types.ETH_TYPE_IP,
                        ip_proto=in_proto.IPPROTO_TCP,
                        ipv4_src=self.SERVER2['ip'],
                        ipv4_dst=self.CLIENT['ip'],
                    )
                    actions_server2_to_client = [
                        parser.OFPActionSetField(eth_src=self.SERVER1['mac']),
                        parser.OFPActionSetField(ipv4_src=self.SERVER1['ip']),
                        parser.OFPActionOutput(client_port),
                    ]
                    self.add_flow(datapath, 1, match_server2_to_client, actions_server2_to_client)

                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data

                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=msg.buffer_id, 
                    in_port=client_port,
                    actions=actions_client_to_server2,
                    data=data
                )
                datapath.send_msg(out)
                return  


        #check ICMP Protocol 
        if protocol == in_proto.IPPROTO_ICMP:
            match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP,
                                            ipv4_src=ip_pkt.src, ipv4_dst=ip_pkt.dst, ip_proto=protocol)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                    self.add_flow(datapath, 1, match, actions,msg.buffer_id)
                    return
            else:
                    self.add_flow(datapath, 1, match, actions)
        

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id, 
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(out)
