package org.iris4sdn.csdncm.dhcpserver;

import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.*;
import org.iris4sdn.csdncm.vnetmanager.BridgeHandler;
import org.iris4sdn.csdncm.vnetmanager.OpenstackNode;
import org.iris4sdn.csdncm.vnetmanager.VnetManagerService;
import org.jboss.netty.util.Timeout;
import org.onlab.packet.*;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.Objective;
import org.onosproject.net.host.HostEvent;
import org.onosproject.net.host.HostListener;
import org.onosproject.net.host.HostService;
import org.onosproject.net.packet.DefaultOutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.LogicalClockService;
import org.onosproject.store.service.StorageService;
import org.onosproject.vtnrsc.*;
import org.onosproject.vtnrsc.subnet.SubnetService;
import org.onosproject.vtnrsc.virtualport.VirtualPortService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.onlab.packet.MacAddress.valueOf;
import static org.onlab.util.Tools.groupedThreads;

@Component(immediate = true)
@Service
public class DhcpServerServerManager implements DhcpServerService {
    private final Logger log = LoggerFactory.getLogger(DhcpServerServerManager.class);

    private static final ProviderId PID = new ProviderId("of", DHCPSERVER_APP_ID, true);
    private static final String EVENT_NOT_NULL = "VirtualMachine event cannot be null";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected NetworkConfigRegistry cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected VnetManagerService vnetManagerService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected SubnetService subnetService;

    private DhcpPacketProcessor processor = new DhcpPacketProcessor();
    private HostListener hostListener = new InnerHostListener();

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected VirtualPortService virtualPortService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LogicalClockService clockService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    private EventuallyConsistentMap<Host, FixedIp> hostStore;

    private final ExecutorService eventExecutor = Executors
            .newFixedThreadPool(1, groupedThreads("onos/dhcpmanager", "event-handler"));

    private static final String ALLOCATIONPOOL_IN_SUBNET = "allocationpool-in-subnet";
    private static final String ALLOCATIONPOOL_IN_HOST = "allocationpool-in-host";
    private static Ip4Address myIP = Ip4Address.valueOf("10.0.0.51");
    private static MacAddress myMAC = valueOf("68:05:ca:3c:28:a9");
    private static int leaseTime = 600;
    private static int renewalTime = 300;
    private static int rebindingTime = 360;
    private static byte packetTTL = (byte) 127;
    private static Ip4Address subnetMask = Ip4Address.valueOf("255.255.255.0");
    private static Ip4Address broadcastAddress = Ip4Address.valueOf("1.1.1.1");
    private static Ip4Address routerAddress = Ip4Address.valueOf("10.0.8.1");
    private static Ip4Address domainServer = Ip4Address.valueOf("10.0.0.51");
    private static final Ip4Address IP_BROADCAST = Ip4Address.valueOf("255.255.255.255");

    protected Timeout timeout;
    protected static int timerDelay = 2;
    //private final InternalConfigListener cfgListener = new InternalConfigListener();
    private static DhcpRuleInstaller installer;
    private ApplicationId appId;
    private static final String IFACEID = "ifaceid";


    @Activate
    protected void activate() {
        appId = coreService.registerApplication(DHCPSERVER_APP_ID);

        KryoNamespace.Builder serializer = KryoNamespace.newBuilder()
                .register(KryoNamespaces.API)
                .register(Subnet.class)
                .register(Host.class)
                .register(FixedIp.class);

        hostStore = storageService
                .<Host, FixedIp>eventuallyConsistentMapBuilder()
                .withName(ALLOCATIONPOOL_IN_HOST).withSerializer(serializer)
                .withTimestampProvider((k, v) -> clockService.getTimestamp())
                .build();

        installer = DhcpRuleInstaller.ruleInstaller(appId);
        packetService.addProcessor(processor, PacketProcessor.director(0));
        hostService.addListener(hostListener);
        log.info("Started!!");
    }

    @Deactivate
    protected void deactivate() {
        packetService.removeProcessor(processor);
        hostService.removeListener(hostListener);
        eventExecutor.shutdown();
        log.info("Stopped");
    }


    private class DhcpPacketProcessor implements PacketProcessor {

        private Ethernet buildReply(Ethernet packet, Ip4Address ipOffered, byte outgoingMessageType, Ip4Address gatewayIp) {
            Ip4Address subnetMaskReply;
            Ip4Address dhcpServerReply;
            Ip4Address routerAddressReply;
            Ip4Address domainServerReply;

            subnetMaskReply = subnetMask;
            dhcpServerReply = myIP;
            routerAddressReply = gatewayIp;

            domainServerReply = domainServer;
            log.info("subnet {}", subnetMaskReply);
            log.info("dhcp server reply {}", dhcpServerReply);
            log.info("router address reply {}", routerAddressReply);
            log.info("domain server reply {}", domainServerReply);

            // Ethernet Frame.
            Ethernet ethReply = new Ethernet();
            ethReply.setSourceMACAddress(myMAC);
            MacAddress mac = valueOf("ff:ff:ff:ff:ff:ff");
            ethReply.setDestinationMACAddress(mac);
            //ethReply.setDestinationMACAddress(packet.getSourceMAC());
            ethReply.setEtherType(Ethernet.TYPE_IPV4);
            ethReply.setVlanID(packet.getVlanID());

            // IP Packet
            IPv4 ipv4Packet = (IPv4) packet.getPayload();
            IPv4 ipv4Reply = new IPv4();
            ipv4Reply.setSourceAddress(dhcpServerReply.toInt());
            ipv4Reply.setDestinationAddress(ipOffered.toInt());
            ipv4Reply.setTtl(packetTTL);

            // UDP Datagram.
            UDP udpPacket = (UDP) ipv4Packet.getPayload();
            UDP udpReply = new UDP();
            udpReply.setSourcePort((byte) UDP.DHCP_SERVER_PORT);
            udpReply.setDestinationPort((byte) UDP.DHCP_CLIENT_PORT);

            // DHCP Payload.
            DHCP dhcpPacket = (DHCP) udpPacket.getPayload();
            DHCP dhcpReply = new DHCP();
            dhcpReply.setOpCode(DHCP.OPCODE_REPLY);
            dhcpReply.setFlags(dhcpPacket.getFlags());
            dhcpReply.setGatewayIPAddress(dhcpPacket.getGatewayIPAddress());
            dhcpReply.setClientHardwareAddress(dhcpPacket.getClientHardwareAddress());
            dhcpReply.setTransactionId(dhcpPacket.getTransactionId());

            if (outgoingMessageType != DHCPPacketType.DHCPNAK.getValue()) {
                dhcpReply.setYourIPAddress(ipOffered.toInt());
                dhcpReply.setServerIPAddress(dhcpServerReply.toInt());
                if (dhcpPacket.getGatewayIPAddress() == 0) {
                    ipv4Reply.setDestinationAddress(IP_BROADCAST.toInt());
                }
            }
            dhcpReply.setHardwareType(DHCP.HWTYPE_ETHERNET);
            dhcpReply.setHardwareAddressLength((byte) 6);

            // DHCP Options.
            DHCPOption option = new DHCPOption();

            List<DHCPOption> optionList = new ArrayList<>();

            // DHCP Message Type.
            option.setCode(DHCP.DHCPOptionCode.OptionCode_MessageType.getValue());
            option.setLength((byte) 1);
            byte[] optionData = {outgoingMessageType};
            option.setData(optionData);
            optionList.add(option);

            // DHCP Server Identifier.
            option = new DHCPOption();
            option.setCode(DHCP.DHCPOptionCode.OptionCode_DHCPServerIp.getValue());
            option.setLength((byte) 4);
            option.setData(dhcpServerReply.toOctets());
            optionList.add(option);

            if (outgoingMessageType != DHCPPacketType.DHCPNAK.getValue()) {
                // IP Address Lease Time.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_LeaseTime.getValue());
                option.setLength((byte) 4);
                option.setData(ByteBuffer.allocate(4)
                        .putInt(leaseTime).array());
                optionList.add(option);

                // IP Address Renewal Time.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_RenewalTime.getValue());
                option.setLength((byte) 4);
                option.setData(ByteBuffer.allocate(4).putInt(renewalTime).array());
                optionList.add(option);

                // IP Address Rebinding Time.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OPtionCode_RebindingTime.getValue());
                option.setLength((byte) 4);
                option.setData(ByteBuffer.allocate(4).putInt(rebindingTime).array());
                optionList.add(option);

                // Subnet Mask.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_SubnetMask.getValue());
                option.setLength((byte) 4);
                option.setData(subnetMaskReply.toOctets());
                optionList.add(option);

                // Broadcast Address.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_BroadcastAddress.getValue());
                option.setLength((byte) 4);
                option.setData(broadcastAddress.toOctets());
                optionList.add(option);

                // Router Address.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_RouterAddress.getValue());
                option.setLength((byte) 4);
                option.setData(routerAddressReply.toOctets());
                optionList.add(option);

                // DNS Server Address.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_DomainServer.getValue());
                option.setLength((byte) 4);
                option.setData(domainServerReply.toOctets());
                optionList.add(option);
            }

            // End Option.
            option = new DHCPOption();
            option.setCode(DHCP.DHCPOptionCode.OptionCode_END.getValue());
            option.setLength((byte) 1);
            optionList.add(option);

            dhcpReply.setOptions(optionList);
            udpReply.setPayload(dhcpReply);
            ipv4Reply.setPayload(udpReply);
            ethReply.setPayload(ipv4Reply);

            return ethReply;
        }

        private void sendReply(PacketContext context, Ethernet reply) {
            log.info("sendReply out");
            if (reply != null) {
                log.info("sendReply in");
                TrafficTreatment.Builder builder = DefaultTrafficTreatment.builder();
                ConnectPoint sourcePoint = context.inPacket().receivedFrom();
                builder.setOutput(sourcePoint.port());
                context.block();
                packetService.emit(new DefaultOutboundPacket(sourcePoint.deviceId(),
                        builder.build(), ByteBuffer.wrap(reply.serialize())));
            }
        }

        private void processDhcpPacket(PacketContext context, DHCP dhcpPayload) {
            Ethernet packet = context.inPacket().parsed();
            boolean flagIfRequestedIP = false;
            boolean flagIfServerIP = false;
            Ip4Address requestedIP = Ip4Address.valueOf("0.0.0.0");
            Ip4Address serverIP = Ip4Address.valueOf("0.0.0.0");
            Subnet subnet;
            Ip4Address gatewayIP = null;

            if (dhcpPayload != null) {
                DHCPPacketType incomingPacketType = DHCPPacketType.getType(0);
                for (DHCPOption option : dhcpPayload.getOptions()) {
                    if (option.getCode() == DHCP.DHCPOptionCode.OptionCode_MessageType.getValue()) {
                        byte[] data = option.getData();
                        incomingPacketType = DHCPPacketType.getType(data[0]);
                    }
                    if (option.getCode() == DHCP.DHCPOptionCode.OptionCode_RequestedIP.getValue()) {
                        byte[] data = option.getData();
                        requestedIP = Ip4Address.valueOf(data);
                        flagIfRequestedIP = true;
                    }
                    if (option.getCode() == DHCP.DHCPOptionCode.OptionCode_DHCPServerIp.getValue()) {
                        byte[] data = option.getData();
                        serverIP = Ip4Address.valueOf(data);
                        flagIfServerIP = true;
                    }
                }
                DHCPPacketType outgoingPacketType;

                MacAddress clientMac = new MacAddress(dhcpPayload.getClientHardwareAddress());
                VlanId vlanId = VlanId.vlanId(packet.getVlanID());
                log.info("clientMac {}", clientMac);
                log.info("vlanId {}", vlanId);
                HostId hostId = HostId.hostId(clientMac, vlanId);
                Host host = hostService.getHost(hostId);

                FixedIp fixedIp= hostStore.get(host);
                SubnetId subnetid = fixedIp.subnetId();
                subnet = subnetService.getSubnet(subnetid);

                gatewayIP = Ip4Address.valueOf(subnet.gatewayIp().toString());
                requestedIP = Ip4Address.valueOf(fixedIp.ip().toString());

                if (incomingPacketType.getValue() == DHCPPacketType.DHCPDISCOVER.getValue()) {
                    outgoingPacketType = DHCPPacketType.DHCPOFFER;
                    Ip4Address ipOffered = null;
                    ipOffered = requestedIP;

                    if (ipOffered != null) {
                        Ethernet ethReply = buildReply(packet, ipOffered,
                                (byte) outgoingPacketType.getValue(),gatewayIP);
                        log.info("ethReply {}", ethReply);
                        sendReply(context, ethReply);
                    }
                    log.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5ipOffered {}",ipOffered);
                }
                else if (incomingPacketType.getValue() == DHCPPacketType.DHCPREQUEST.getValue()) {
                    log.info("@@@DHCP REQUEST ************************************");
                    log.info("server IP {}", serverIP);
                    log.info("packet {}", packet);

                    if (flagIfServerIP && flagIfRequestedIP) {
                        if (myIP.equals(serverIP)) {
                            outgoingPacketType = DHCPPacketType.DHCPACK;
                            log.info("hahahah");

                            Ethernet ethReply = buildReply(packet, requestedIP,
                                    (byte) outgoingPacketType.getValue(), gatewayIP);
                            sendReply(context, ethReply);
                        }
                    }
                }
            }
        }


        @Override
        public void process(PacketContext context) {
            Ethernet packet = context.inPacket().parsed();
            if (packet == null) {
                return;
            }
            if (packet.getEtherType() == Ethernet.TYPE_IPV4) {
                IPv4 ipv4Packet = (IPv4) packet.getPayload();
                if (ipv4Packet.getProtocol() == IPv4.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv4Packet.getPayload();
                    if (udpPacket.getDestinationPort() == UDP.DHCP_SERVER_PORT &&
                            udpPacket.getSourcePort() == UDP.DHCP_CLIENT_PORT) {
                        DHCP dhcpPayload = (DHCP) udpPacket.getPayload();
                        processDhcpPacket(context, dhcpPayload);
                    }
                }
            }
        }
    }


    private void getIp(Host host){
        String ifaceId = host.annotations().value(IFACEID);
        if (ifaceId == null) {
            log.error("The ifaceId of Host is null");
            return;
        }

        VirtualPortId virtualPortId = VirtualPortId.portId(ifaceId);
        VirtualPort virtualPort = virtualPortService.getPort(virtualPortId);
        if (virtualPort == null) {
            log.error("Could not find virutal port of the host {}", host.toString());
            return;
        }

        FixedIp fixedIp = null;

        if (virtualPort.fixedIps().size() > 0) {
            Set<FixedIp> floating_ips = virtualPort.fixedIps();
            for (FixedIp floating_ip : floating_ips) {
                fixedIp = floating_ip;
            }
        }
        hostStore.put(host,fixedIp);
    }


    private void processHost(Host host, Objective.Operation operation) {
        //log.info("Host {} processed", host);
        getIp(host);
        IpAddress dstIpAddress = Ip4Address.valueOf("255.255.255.255");
        IpAddress srcIpAddress = Ip4Address.valueOf("0.0.0.0");
        MacAddress dstMacAddress = valueOf("ff:ff:ff:ff:ff:ff");
        MacAddress srcMacAddress = host.mac();
        Sets.newHashSet(vnetManagerService.getOpenstackNodes()).stream()
                .filter(e -> e.getState().contains(OpenstackNode.State.BRIDGE_CREATED))
                .forEach(e -> {
                    DeviceId deviceId = e.getBridgeId(BridgeHandler.BridgeType.INTEGRATION);
                    installer.programDhcp(deviceId,  dstIpAddress, srcIpAddress, dstMacAddress,
                            srcMacAddress, operation);
                });
    }

    private class InnerHostListener implements HostListener {
        @Override
        public void event(HostEvent event) {
            Host host = event.subject();
            if (HostEvent.Type.HOST_ADDED == event.type()) {
                eventExecutor.submit(() -> processHost(host, Objective.Operation.ADD));
            } else if (HostEvent.Type.HOST_REMOVED == event.type()) {
                eventExecutor.submit(() -> processHost(host, Objective.Operation.REMOVE));
            } else if (HostEvent.Type.HOST_UPDATED == event.type()) {
                eventExecutor.submit(() -> processHost(host, Objective.Operation.REMOVE));
                eventExecutor.submit(() -> processHost(host, Objective.Operation.ADD));
            }
        }
    }
}
