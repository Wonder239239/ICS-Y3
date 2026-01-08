from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, in_proto
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.lib.packet import ipv4, tcp

class RyuForward(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    
    def __init__(self, *args, **kwargs):
        super(RyuForward, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
    
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
        #self.logger.info("packet in from src %s to dst %s at dpid %s in_port %s", src, dst, dpid, in_port)
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD


        actions = [parser.OFPActionOutput(out_port)]

  
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        protocol=ip_pkt.proto if ip_pkt else None
        #check TCP Protocol and IPV4 protocol
        if ip_pkt and protocol == in_proto.IPPROTO_TCP and tcp_pkt:
            #check TCP SYN
            if (tcp_pkt.bits & tcp.TCP_SYN) and not (tcp_pkt.bits & tcp.TCP_ACK) and out_port != ofproto.OFPP_FLOOD:
                self.logger.info("Detected TCP SYN, installing flow entry")
                match = parser.OFPMatch(
                    eth_type=ether_types.ETH_TYPE_IP,
                    ip_proto=protocol,
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst,
                    ipv4_src=ip_pkt.src,
                    ipv4_dst=ip_pkt.dst,
                )


                match_reverse = parser.OFPMatch(
                        in_port=out_port,      
                        eth_type=ether_types.ETH_TYPE_IP,
                        ip_proto=protocol,
                        eth_src=dst,
                        eth_dst=src,
                        ipv4_src=ip_pkt.dst,    
                        ipv4_dst=ip_pkt.src    
                    )
                actions_reverse = [parser.OFPActionOutput(in_port)] 
                self.add_flow(datapath, 1, match_reverse, actions_reverse, buffer_id=None)





                if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                    self.add_flow(datapath, 1, match, actions, buffer_id=msg.buffer_id)
                    return
                else:
                    self.add_flow(datapath, 1, match, actions, buffer_id=None)

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
        #self.logger.info("packet out from src %s to dst %s at dpid %s in_port %s", src, dst, dpid, in_port)
        datapath.send_msg(out)
