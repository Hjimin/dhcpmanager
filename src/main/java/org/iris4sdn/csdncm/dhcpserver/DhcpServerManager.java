package org.iris4sdn.csdncm.dhcpserver;

import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.*;
import org.iris4sdn.csdncm.vnetmanager.Bridge;
import org.iris4sdn.csdncm.vnetmanager.NodeManagerService;
import org.iris4sdn.csdncm.vnetmanager.OpenstackNode;
import org.onlab.packet.*;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
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
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.LogicalClockService;
import org.onosproject.store.service.StorageService;
import org.onosproject.vtnrsc.*;
import org.onosproject.vtnrsc.service.VtnRscService;
import org.onosproject.vtnrsc.subnet.SubnetService;
import org.onosproject.vtnrsc.tenantnetwork.TenantNetworkService;
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

import static com.google.common.base.Preconditions.checkNotNull;

@Component(immediate = true)
@Service
public class DhcpServerManager implements DhcpServerService {
    private final Logger log = LoggerFactory.getLogger(DhcpServerManager.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected NodeManagerService nodeManagerService;

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

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected VtnRscService vtnRscService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected TenantNetworkService tenantNetworkService;


    private EventuallyConsistentMap<Host, FixedIp> hostStore;

    private final ExecutorService eventExecutor = Executors
            .newFixedThreadPool(1, groupedThreads("onos/dhcpmanager", "event-handler"));

    private static final String ALLOCATIONPOOL_IN_HOST = "allocationpool-in-host";
    private static Ip4Address CONTROLLER_IP = Ip4Address.valueOf("192.168.10.5");
    private static MacAddress CONTROLLER_MAC = valueOf("68:05:ca:3c:28:a0");
    private static int leaseTime = 600;
    private static int renewalTime = 300;
    private static int rebindingTime = 360;
    private static byte packetTTL = (byte) 127;
    private static Ip4Address subnetMask = Ip4Address.valueOf("255.255.255.0");
    private static Ip4Address srcAddress = Ip4Address.valueOf("192.168.10.5");
    private static Ip4Address domainServer = Ip4Address.valueOf("192.168.10.5");

    private static final Ip4Address IP_BROADCAST = Ip4Address.valueOf("255.255.255.255");
    private static final Ip4Address DEFAULT_IP = Ip4Address.valueOf("0.0.0.0");
    private static final MacAddress MAC_BROADCAST = MacAddress.valueOf("ff:ff:ff:ff:ff:ff");


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
        log.info("Started");
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
            Ip4Address broadcastAddressReply;

            subnetMaskReply = subnetMask;
            dhcpServerReply = CONTROLLER_IP;
            routerAddressReply = gatewayIp;
            String gate = gatewayIp.toString();
            String broad = gate.substring(0,gate.lastIndexOf(".")) + ".255";
            broadcastAddressReply = Ip4Address.valueOf(broad);
            domainServerReply = domainServer;

//            log.info("subnet {}", subnetMaskReply);
//            log.info("dhcp server reply {}", dhcpServerReply);
//            log.info("router address reply {}", routerAddressReply);
//            log.info("domain server reply {}", domainServerReply);

            // Ethernet Frame.
            Ethernet ethReply = new Ethernet();
            ethReply.setSourceMACAddress(CONTROLLER_MAC);
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
                option.setData(broadcastAddressReply.toOctets());
                optionList.add(option);

//                option = new DHCPOption();
//                option.setCode((byte) 15);
//                option.setLength((byte) 14);
//                String domain_name = "openstacklocal";
//                option.setData(domain_name.getBytes());
//                optionList.add(option);
//
//                option = new DHCPOption();
//                option.setCode((byte) 12);
//                option.setLength((byte) 13);
//                option.setData(domain_name.getBytes());
//                optionList.add(option);

                // DNS Server Address.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_DomainServer.getValue());
                option.setLength((byte) 4);
                option.setData(domainServerReply.toOctets());
                optionList.add(option);

                // Router Address.
                option = new DHCPOption();
                option.setCode(DHCP.DHCPOptionCode.OptionCode_RouterAddress.getValue());
                option.setLength((byte) 4);
                option.setData(routerAddressReply.toOctets());
                optionList.add(option);

                option = new DHCPOption();
                option.setCode((byte) 121);
                option.setLength((byte) 14);
                //option.setData(0x0e20a9fea9fe0a000202000a0002011a0205aeff);
                ByteBuffer bb = ByteBuffer.allocate(14);
                //bb.put((byte)0x000a000001);

                bb.put((byte)0x20);

                bb.put((byte)0xa9);
                bb.put((byte)0xfe);
                bb.put((byte)0xa9);
                bb.put((byte)0xfe);

                //gateway
                bb.put(gatewayIp.toOctets()[0]);
                bb.put(gatewayIp.toOctets()[1]);
                bb.put(gatewayIp.toOctets()[2]);
                bb.put(gatewayIp.toOctets()[3]);
//                bb.put((byte)0x0a);
//                bb.put((byte)0x00);
//                bb.put((byte)0x00);
//                bb.put((byte)0x01);

                bb.put((byte)0x00);
                bb.put(gatewayIp.toOctets()[0]);
                bb.put(gatewayIp.toOctets()[1]);
                bb.put(gatewayIp.toOctets()[2]);
                bb.put(gatewayIp.toOctets()[3]);
//                bb.put((byte)0xa9fea9fe);
//                  bb.put((byte)0x0a000033);
                option.setData(bb.array());
                //option.setData(broadcastAddress.toOctets());
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
            TrafficTreatment.Builder builder = DefaultTrafficTreatment.builder();
            ConnectPoint sourcePoint = context.inPacket().receivedFrom();
            builder.setOutput(sourcePoint.port());
            context.block();
            packetService.emit(new DefaultOutboundPacket(sourcePoint.deviceId(),
                    builder.build(), ByteBuffer.wrap(reply.serialize())));
        }

        private void processDhcpPacket(PacketContext context, DHCP dhcpPayload) {
            Ethernet packet = context.inPacket().parsed();
            boolean flagIfRequestedIP = false;
            boolean flagIfServerIP = false;
            Ip4Address requestedIP = Ip4Address.valueOf("0.0.0.0");
            Ip4Address serverIP = Ip4Address.valueOf("192.168.10.5");
            Ip4Address gatewayIP = null;

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


            MacAddress clientMac = new MacAddress(dhcpPayload.getClientHardwareAddress());
            VlanId vlanId = VlanId.vlanId(packet.getVlanID());
            HostId hostId = HostId.hostId(clientMac, vlanId);
            Host host = hostService.getHost(hostId);
            if(host == null) {
                log.info("{} is not part of management");
                return;
            }

            FixedIp fixedIp = hostStore.get(host);
            if(fixedIp == null) {
                log.warn("fixedIp should not be null");
                return;
            }

            SubnetId subnetid = fixedIp.subnetId();
            if(subnetid == null) {
                log.warn("sunetid should not be null");
                return;
            }
            Subnet subnet = subnetService.getSubnet(subnetid);

            gatewayIP = Ip4Address.valueOf(subnet.gatewayIp().toString());
            requestedIP = Ip4Address.valueOf(fixedIp.ip().toString());

            if (incomingPacketType.getValue() == DHCPPacketType.DHCPDISCOVER.getValue()) {
                DHCPPacketType outgoingPacketType = DHCPPacketType.DHCPOFFER;
                Byte messageType = (byte)outgoingPacketType.getValue();
                Ip4Address ipOffered = requestedIP;

                if (ipOffered != null) {
                    Ethernet ethReply = buildReply(packet, ipOffered, messageType,gatewayIP);
                    checkNotNull(ethReply, "ethReply should not be null");
                    sendReply(context, ethReply);
                }
            } else if (incomingPacketType.getValue() == DHCPPacketType.DHCPREQUEST.getValue()) {
                if (flagIfServerIP && flagIfRequestedIP) {
                    if (CONTROLLER_IP.equals(serverIP)) {
                        DHCPPacketType outgoingPacketType = DHCPPacketType.DHCPACK;
                        Byte messageType = (byte)outgoingPacketType.getValue();
                        Ethernet ethReply = buildReply(packet, requestedIP, messageType, gatewayIP);
                        checkNotNull(ethReply, "ethReply should not be null");
                        sendReply(context, ethReply);
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

                        if(dhcpPayload == null) {
                            return;
                        }

                        MacAddress client_macAddress = MacAddress.valueOf(dhcpPayload.getClientHardwareAddress());
                        Sets.newHashSet(hostService.getHosts()).stream()
                                .filter(host -> (host.mac().toString().equals(client_macAddress.toString())))
                                .forEach(host -> processDhcpPacket(context, dhcpPayload));
                    }
                }
            }
        }
    }


    private void configIp(Host host){
        String ifaceId = null;
        for (int i = 0; i < 5; i++) {
            ifaceId = host.annotations().value(IFACEID);
            if (ifaceId == null) {
                try {
                  // Need to wait for synchronising
                          Thread.sleep(500);
                } catch (InterruptedException exeption) {
                  log.warn("Interrupted while waiting to get ifaceId");
                  //Thread.currentThread().interrupt();
                }
            } else  {
                break;
            }
        }

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
        if (fixedIp == null) {
            log.error("fixedIp should not be null");
            return;
        }
        hostStore.put(host,fixedIp);
    }

    private SegmentationId getSegmentationId(Host host) {
        String ifaceId = host.annotations().value(IFACEID);
        if (ifaceId == null) {
            log.error("The ifaceId of Host is null");
            return null;
        }

        VirtualPortId virtualPortId = VirtualPortId.portId(ifaceId);
        VirtualPort virtualPort = virtualPortService.getPort(virtualPortId);
        if (virtualPort == null) {
            log.error("Could not find virutal port of the host {}", host.toString());
            return null;
        }

        // Add virtual port information
        TenantNetwork tenantNetwork = tenantNetworkService.getNetwork(virtualPort.networkId());
        SegmentationId segmentationId = tenantNetwork.segmentationId();
        return segmentationId;
    }


    private void processHost(Host host, Objective.Operation operation) {
        IpAddress dstIpAddress = IP_BROADCAST;
        IpAddress srcIpAddress = DEFAULT_IP;
        MacAddress dstMacAddress = MAC_BROADCAST;
        configIp(host);
        SegmentationId segmentationId = getSegmentationId(host);
        if(segmentationId == null) {
            log.error("SegmentaionId should not be null");
            return;
        }
        Sets.newHashSet(nodeManagerService.getOpenstackNodes()).stream()
                .filter(e -> e.getState().contains(OpenstackNode.State.BRIDGE_CREATED))
                .forEach(e -> {
                    DeviceId deviceId = e.getBridgeId(Bridge.BridgeType.INTEGRATION);
                    installer.programDhcp(deviceId, dstIpAddress, segmentationId,
                            srcIpAddress, dstMacAddress, operation);
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
