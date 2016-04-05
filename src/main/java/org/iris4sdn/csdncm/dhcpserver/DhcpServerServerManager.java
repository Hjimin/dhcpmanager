package org.iris4sdn.csdncm.dhcpserver;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import org.onosproject.net.config.ConfigFactory;
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
import org.onosproject.vtnrsc.tenantnetwork.TenantNetworkEvent;
import org.onosproject.vtnrsc.tenantnetwork.TenantNetworkListener;
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
import static org.onosproject.net.config.basics.SubjectFactories.APP_SUBJECT_FACTORY;

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
    protected DhcpServerStore dhcpServerStore;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected SubnetService subnetService;

    private DhcpPacketProcessor processor = new DhcpPacketProcessor();
    private HostListener hostListener = new InnerHostListener();

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected VirtualPortService virtualPortService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LogicalClockService clockService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected TenantNetworkService tenantNetworkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    private EventuallyConsistentMap<SubnetId, AllocationPool> subnetStore;

    private final ExecutorService eventExecutor = Executors
            .newFixedThreadPool(1, groupedThreads("onos/dhcpmanager", "event-handler"));

    private static final String ALLOCATIONPOOL_IN_SUBNET = "allocationpool-in-subnet";
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
    private final Set<ConfigFactory> factories = ImmutableSet.of(
            new ConfigFactory<ApplicationId, DhcpServerConfig>(APP_SUBJECT_FACTORY,
                    DhcpServerConfig.class,
                    "dhcp") {
                @Override
                public DhcpServerConfig createConfig() {
                    return new DhcpServerConfig();
                }
            }
    );

    private TenantNetworkListener tenantNetworkListener = new InnerTenantNetworkListener();

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(DHCPSERVER_APP_ID);
        //cfgService.addListener(cfgListener);
        factories.forEach(cfgService::registerConfigFactory);

        KryoNamespace.Builder serializer = KryoNamespace.newBuilder()
                .register(KryoNamespaces.API)
                .register(Subnet.class);
        subnetStore = storageService
                .<SubnetId, AllocationPool>eventuallyConsistentMapBuilder()
                .withName(ALLOCATIONPOOL_IN_SUBNET).withSerializer(serializer)
                .withTimestampProvider((k, v) -> clockService.getTimestamp())
                .build();

        //what is director
        installer = DhcpRuleInstaller.ruleInstaller(appId);
        packetService.addProcessor(processor, PacketProcessor.director(0));
        //timeout = Timer.getTimer().newTimeout(new PurgeListTask(), timerDelay, TimeUnit.MINUTES);
        hostService.addListener(hostListener);
        tenantNetworkService.addListener(tenantNetworkListener);
        log.info("Started!!");
    }

    @Deactivate
    protected void deactivate() {
        packetService.removeProcessor(processor);
        //cfgService.removeListener(cfgListener);
        factories.forEach(cfgService::unregisterConfigFactory);
        //timeout.cancel();
        subnetStore.destroy();
        hostService.removeListener(hostListener);
        tenantNetworkService.removeListener(tenantNetworkListener);
        eventExecutor.shutdown();
        log.info("Stopped");
    }


    private class DhcpPacketProcessor implements PacketProcessor {

        private Ethernet buildReply(Ethernet packet, Ip4Address ipOffered, byte outgoingMessageType, Ip4Address gatewayIp) {
            Ip4Address subnetMaskReply;
            Ip4Address dhcpServerReply;
            Ip4Address routerAddressReply;
            Ip4Address domainServerReply;
            IpAssignmentHandler ipAssignment;

            ipAssignment = dhcpServerStore.getIpAssignmentFromAllocationMap(HostId.hostId(packet.getSourceMAC()));

            if (ipAssignment != null && ipAssignment.rangeNotEnforced()) {
                log.info("11111");
                subnetMaskReply = ipAssignment.subnetMask();
                dhcpServerReply = ipAssignment.dhcpServer();
                domainServerReply = ipAssignment.domainServer();
                routerAddressReply = ipAssignment.routerAddress();
            } else {
                log.info("22222");
                subnetMaskReply = subnetMask;
                dhcpServerReply = myIP;
                    routerAddressReply = gatewayIp;

                domainServerReply = domainServer;
                log.info("subnet {}", subnetMaskReply);
                log.info("dhcp server reply {}", dhcpServerReply);
                log.info("router address reply {}", routerAddressReply);
                log.info("domain server reply {}", domainServerReply);
            }

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
                        .putInt(ipAssignment == null ? leaseTime : ipAssignment.leasePeriod()).array());
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
                subnet = getSubnet(host);
                gatewayIP = Ip4Address.valueOf(getSubnet(host).gatewayIp().toString());
                if (incomingPacketType.getValue() == DHCPPacketType.DHCPDISCOVER.getValue()) {

                    outgoingPacketType = DHCPPacketType.DHCPOFFER;
                    Ip4Address ipOffered = null;
                    ipOffered = dhcpServerStore.suggestIP(hostId, requestedIP, getSubnet(host));



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
                        // SELECTING state


                        if (dhcpServerStore.getIpAssignmentFromAllocationMap(HostId.hostId(clientMac)).rangeNotEnforced() == false){
                            log.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa");
                        }
                        if (dhcpServerStore.getIpAssignmentFromAllocationMap(HostId.hostId(clientMac))
                                .rangeNotEnforced()) {
                            outgoingPacketType = DHCPPacketType.DHCPACK;
                            Ethernet ethReply = buildReply(packet, requestedIP, (byte) outgoingPacketType.getValue(), gatewayIP);
                            sendReply(context, ethReply);
                        } else {
                            if (myIP.equals(serverIP)) {
                                if (dhcpServerStore.assignIP(hostId, requestedIP, leaseTime, false, Lists.newArrayList(),subnet) == false) {
                                    log.info("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBbba");
                                    return;
                                }
                                if (dhcpServerStore.assignIP(hostId, requestedIP, leaseTime, false, Lists.newArrayList(),subnet)) {
                                    outgoingPacketType = DHCPPacketType.DHCPACK;
                                    log.info("hahahah");
                                    //discoverHost(context, requestedIP);
                                } else {
                                    outgoingPacketType = DHCPPacketType.DHCPNAK;
                                }
                                Ethernet ethReply = buildReply(packet, requestedIP,
                                        (byte) outgoingPacketType.getValue(), gatewayIP);
                                sendReply(context, ethReply);
                            }
                        }
                    } else if (flagIfRequestedIP) {
                        // INIT-REBOOT state
                        if (dhcpServerStore.assignIP(hostId, requestedIP, leaseTime, false, Lists.newArrayList(),subnet)) {
                            outgoingPacketType = DHCPPacketType.DHCPACK;
                            log.info("gate way!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! {}", gatewayIP);
                            Ethernet ethReply = buildReply(packet, requestedIP, (byte) outgoingPacketType.getValue(), gatewayIP);
                            sendReply(context, ethReply);
                            //discoverHost(context, requestedIP);
                        }

                    } else {
                        // RENEWING and REBINDING state
                        int ciaadr = dhcpPayload.getClientIPAddress();
                        if (ciaadr != 0) {
                            Ip4Address clientIaddr = Ip4Address.valueOf(ciaadr);
                            if (dhcpServerStore.assignIP(hostId, clientIaddr, leaseTime, false, Lists.newArrayList(), subnet)) {
                                outgoingPacketType = DHCPPacketType.DHCPACK;
                                //discoverHost(context, clientIaddr);
                            } else if (packet.getEtherType() == Ethernet.TYPE_IPV4 &&
                                    ((IPv4) packet.getPayload()).getDestinationAddress() == myIP.toInt()) {
                                outgoingPacketType = DHCPPacketType.DHCPNAK;
                            } else {
                                return;
                            }
                            Ethernet ethReply = buildReply(packet, clientIaddr, (byte) outgoingPacketType.getValue(), gatewayIP);
                            sendReply(context, ethReply);
                        }
                    }

                } else if (incomingPacketType.getValue() == DHCPPacketType.DHCPRELEASE.getValue()) {
                    Ip4Address ip4Address = dhcpServerStore.releaseIP(hostId);
                    if (ip4Address != null) {
                        //hostProviderService.removeIpFromHost(hostId, ip4Address);
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
                        // This is meant for the dhcp server so process the packet here.
                        DHCP dhcpPayload = (DHCP) udpPacket.getPayload();
                        processDhcpPacket(context, dhcpPayload);
                    }
                }
            }
        }
    }


    private Subnet getSubnet(Host host){
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

        IpAddress fixedIp = null;

        if (virtualPort.fixedIps().size() > 0) {
            Set<FixedIp> floating_ips = virtualPort.fixedIps();
            for (FixedIp floating_ip : floating_ips) {
                fixedIp = floating_ip.ip();
                log.info("fixed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!111 {}", fixedIp);
            }
        }

        TenantNetwork tenantNetwork = tenantNetworkService.getNetwork(virtualPort.networkId());
        Subnet subnet = Sets.newHashSet(subnetService.getSubnets()).stream()
                .filter(e -> e.tenantId().toString().equals(tenantNetwork.tenantId().toString()))
                .findAny().orElse(null);
        if(subnet == null) {
            log.error("subnet cannot be null");
            return null;
        }

        return subnet;
    }

    private void getPool(Host host){
        //log.info("Host {} processed", host);
        // For remote Openstack VMs beyond gateway

        Subnet subnet = getSubnet(host);
        if(subnet == null){
            return;
        }
        SubnetId subnetId = subnet.id();
        log.info("subnetId {}", subnetId);
        if(!subnetStore.containsKey(subnetId)){
            log.info("hahahahahahhahhahahahah");
            Iterable<AllocationPool> allocationPools = subnet.allocationPools();
            for(AllocationPool allocationPool : allocationPools) {
                Ip4Address start = Ip4Address.valueOf(allocationPool.startIp().toString());
                Ip4Address end = Ip4Address.valueOf(allocationPool.endIp().toString());
                //routerAddress = (Ip4Address)subnet.gatewayIp();
                dhcpServerStore.populateIPPoolfromRange(subnetId,start, end);
                subnetStore.put(subnetId, allocationPool);
            }
        }
    }

    private void processHost(Host host, Objective.Operation operation) {
        //log.info("Host {} processed", host);
        // For remote Openstack VMs beyond gateway

        getPool(host);
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

    private void processTenantNetwork(TenantNetwork tenantNetwork, Objective.Operation operation){
        log.info("Process Tenant");
//        IpAssignmentHandler ipAssignmentHandler;
//
//        Subnet subnet = Sets.newHashSet(subnetService.getSubnets()).stream()
//                .filter(e -> e.tenantId().toString().equals(tenantNetwork.tenantId().toString()))
//                .findAny().orElse(null);
//
//        if(subnet == null) {
//            log.error("subnet cannot be null");
//        }
//
//        SubnetId subnetId = subnet.id();
//        log.info("subnetId {}", subnetId);
//        //routerAddress = Ip4Address.valueOf(subnet.gatewayIp().toString());
//        if(!subnetStore.containsKey(subnetId)){
//            log.info("hahahahahahhahhahahahah");
//            Iterable<AllocationPool> allocationPools = subnet.allocationPools();
//            for(AllocationPool allocationPool : allocationPools) {
//                Ip4Address start = Ip4Address.valueOf(allocationPool.startIp().toString());
//                Ip4Address end = Ip4Address.valueOf(allocationPool.endIp().toString());
//                dhcpServerStore.populateIPPoolfromRange(start, end);
//                subnetStore.put(subnetId, allocationPool);
//            }
//        }
//
//        ipAssignmentHandler = IpAssignmentHandler.builder()
//                .ipAddress(null)
//                .timestamp(new Date())
//                .leasePeriod(leaseTime)
//                .rangeNotEnforced(true)
//                .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_RangeNotEnforced)
//                .subnetMask((Ip4Address) subnet.gatewayIp())
//                .dhcpServer(domainServer)
//                .routerAddress(routerAddress)
//                .domainServer(domainServer)
//                .build();

    }

    private class InnerHostListener implements HostListener {
        @Override
        public void event(HostEvent event) {
            Host host = event.subject();
            if (HostEvent.Type.HOST_ADDED == event.type()) {
                //log.info("Process added host {}", host);
                eventExecutor.submit(() -> processHost(host, Objective.Operation.ADD));
            } else if (HostEvent.Type.HOST_REMOVED == event.type()) {
                //log.info("Process removed host {}", host);
                eventExecutor.submit(() -> processHost(host, Objective.Operation.REMOVE));
            } else if (HostEvent.Type.HOST_UPDATED == event.type()) {
                //log.info("Process updated host {}", host);
                eventExecutor.submit(() -> processHost(host, Objective.Operation.REMOVE));
                eventExecutor.submit(() -> processHost(host, Objective.Operation.ADD));
            }
        }
    }

    private class InnerTenantNetworkListener implements TenantNetworkListener {

        @Override
        public void event(TenantNetworkEvent event) {
            TenantNetwork tenantNetwork = event.subject();
            if (TenantNetworkEvent.Type.TENANT_NETWORK_PUT == event.type()) {
                log.info("Process added tenantNetwork {}", tenantNetwork);
                eventExecutor.submit(() -> processTenantNetwork(tenantNetwork, Objective.Operation.ADD));
            } else if (TenantNetworkEvent.Type.TENANT_NETWORK_DELETE == event.type()) {
                log.info("Process removed tenantNetwork {}", tenantNetwork);
                eventExecutor.submit(() -> processTenantNetwork(tenantNetwork, Objective.Operation.REMOVE));
            } else if (TenantNetworkEvent.Type.TENANT_NETWORK_UPDATE == event.type()) {
                log.info("Process updated tenantNetwork {}", tenantNetwork);
                eventExecutor.submit(() -> processTenantNetwork(tenantNetwork, Objective.Operation.REMOVE));
                eventExecutor.submit(() -> processTenantNetwork(tenantNetwork, Objective.Operation.ADD));
            }
        }
    }

//    private class PurgeListTask implements org.jboss.netty.util.TimerTask {
//
//        @Override
//        public void run(Timeout to) {
//            IpAssignmentHandler ipAssignment;
//            Date dateNow = new Date();
//
//            Map<HostId, IpAssignmentHandler> ipAssignmentMap = dhcpServerStore.listAllMapping();
//            for (Map.Entry<HostId, IpAssignmentHandler> entry: ipAssignmentMap.entrySet()) {
//                ipAssignment = entry.getValue();
//
//                long timeLapsed = dateNow.getTime() - ipAssignment.timestamp().getTime();
//                if ((ipAssignment.assignmentStatus() != IpAssignmentHandler.AssignmentStatus.Option_Expired) &&
//                        (ipAssignment.leasePeriod() > 0) && (timeLapsed > (ipAssignment.leasePeriodMs()))) {
//
//                    Ip4Address ip4Address = dhcpServerStore.releaseIP(entry.getKey());
//                    if (ip4Address != null) {
//                        //hostProviderService.removeIpFromHost(entry.getKey(), ipAssignment.ipAddress());
//                    }
//                }
//            }
//            timeout = org.onlab.util.Timer.getTimer().newTimeout(new PurgeListTask(), timerDelay, TimeUnit.MINUTES);
//        }
//    }
//

//    private class InternalConfigListener implements NetworkConfigListener {
//
//        /**
//         * Reconfigures the DHCP Server according to the configuration parameters passed.
//         *
//         * @param cfg configuration object
//         */
//        private void reconfigureNetwork(DhcpServerConfig cfg) {
//            if (cfg == null) {
//                return;
//            }
//            if (cfg.ip() != null) {
//                myIP = cfg.ip();
//            }
//            if (cfg.mac() != null) {
//                myMAC = cfg.mac();
//            }
//            if (cfg.subnetMask() != null) {
//                subnetMask = cfg.subnetMask();
//            }
//            if (cfg.broadcastAddress() != null) {
//                broadcastAddress = cfg.broadcastAddress();
//            }
//            if (cfg.routerAddress() != null) {
//                routerAddress = cfg.routerAddress();
//            }
//            if (cfg.domainServer() != null) {
//                domainServer = cfg.domainServer();
//            }
//            if (cfg.ttl() != -1) {
//                packetTTL = (byte) cfg.ttl();
//            }
//            if (cfg.leaseTime() != -1) {
//                leaseTime = cfg.leaseTime();
//            }
//            if (cfg.renewTime() != -1) {
//                renewalTime = cfg.renewTime();
//            }
//            if (cfg.rebindTime() != -1) {
//                rebindingTime = cfg.rebindTime();
//            }
//            if (cfg.defaultTimeout() != -1) {
//                dhcpServerStore.setDefaultTimeoutForPurge(cfg.defaultTimeout());
//            }
//            if (cfg.timerDelay() != -1) {
//                timerDelay = cfg.timerDelay();
//            }
//            if ((cfg.startIp() != null) && (cfg.endIp() != null)) {
//               // dhcpServerStore.populateIPPoolfromRange(cfg.startIp(), cfg.endIp());
//            }
//        }
//
//
//        @Override
//        public void event(NetworkConfigEvent event) {
//
//            if ((event.type() == NetworkConfigEvent.Type.CONFIG_ADDED ||
//                    event.type() == NetworkConfigEvent.Type.CONFIG_UPDATED) &&
//                    event.configClass().equals(DhcpServerConfig.class)) {
//
//                DhcpServerConfig cfg = cfgService.getConfig(appId, DhcpServerConfig.class);
//
//                reconfigureNetwork(cfg);
//                log.info("Reconfigured");
//            }
//        }
//    }

    /*
    //not sure
    private class InternalHostProvider extends AbstractProvider implements HostProvider {


         // Creates a provider with the supplier identifier.

        protected InternalHostProvider() {
            super(PID);
        }

        @Override
        public void triggerProbe(Host host) {
            // nothing to do
        }
    }







    private Ethernet buildReply(Ethernet packet, Ip4Address ipOffered, byte outgoingMessageType) {

        Ip4Address subnetMaskReply;
        Ip4Address dhcpServerReply;
        Ip4Address routerAddressReply;
        Ip4Address domainServerReply;
        IpAssignmentHandler ipAssignmentHandler;

        ipAssignmentHandler = dhcpServerStore.getIpAssignmentFromAllocationMap(HostId.hostId(packet.getSourceMAC()));

        if (ipAssignmentHandler != null && ipAssignmentHandler.rangeNotEnforced()) {
            subnetMaskReply = ipAssignmentHandler.subnetMask();
            dhcpServerReply = ipAssignmentHandler.dhcpServer();
            domainServerReply = ipAssignmentHandler.domainServer();
            routerAddressReply = ipAssignmentHandler.routerAddress();
        } else {
            subnetMaskReply = subnetMask;
            dhcpServerReply = myIP;
            routerAddressReply = routerAddress;
            domainServerReply = domainServer;
        }

        // Ethernet Frame.
        Ethernet ethReply = new Ethernet();
        ethReply.setSourceMACAddress(myMAC);
        ethReply.setDestinationMACAddress(packet.getSourceMAC());
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
                    .putInt(ipAssignmentHandler == null ? leaseTime : ipAssignmentHandler.leasePeriod()).array());
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
        if (reply != null) {
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
            HostId hostId = HostId.hostId(clientMac, vlanId);

            if (incomingPacketType.getValue() == DHCPPacketType.DHCPDISCOVER.getValue()) {

                outgoingPacketType = DHCPPacketType.DHCPOFFER;
                Ip4Address ipOffered = null;
                ipOffered = dhcpServerStore.suggestIP(hostId, requestedIP);

                if (ipOffered != null) {
                    Ethernet ethReply = buildReply(packet, ipOffered,
                            (byte) outgoingPacketType.getValue());
                    sendReply(context, ethReply);
                }
            } else if (incomingPacketType.getValue() == DHCPPacketType.DHCPREQUEST.getValue()) {

                if (flagIfServerIP && flagIfRequestedIP) {
                    // SELECTING state


                    if (dhcpServerStore.getIpAssignmentFromAllocationMap(HostId.hostId(clientMac))
                            .rangeNotEnforced()) {
                        outgoingPacketType = DHCPPacketType.DHCPACK;
                        Ethernet ethReply = buildReply(packet, requestedIP, (byte) outgoingPacketType.getValue());
                        sendReply(context, ethReply);
                    } else {
                        if (myIP.equals(serverIP)) {
                            if (dhcpServerStore.assignIP(hostId, requestedIP, leaseTime, false, Lists.newArrayList())) {
                                outgoingPacketType = DHCPPacketType.DHCPACK;
                                //discoverHost(context, requestedIP);
                            } else {
                                outgoingPacketType = DHCPPacketType.DHCPNAK;
                            }
                            Ethernet ethReply = buildReply(packet, requestedIP,
                                    (byte) outgoingPacketType.getValue());
                            sendReply(context, ethReply);
                        }
                    }
                } else if (flagIfRequestedIP) {
                    // INIT-REBOOT state
                    if (dhcpServerStore.assignIP(hostId, requestedIP, leaseTime, false, Lists.newArrayList())) {
                        outgoingPacketType = DHCPPacketType.DHCPACK;
                        Ethernet ethReply = buildReply(packet, requestedIP, (byte) outgoingPacketType.getValue());
                        sendReply(context, ethReply);
                        // discoverHost(context, requestedIP);
                    }

                } else {
                    // RENEWING and REBINDING state
                    int ciaadr = dhcpPayload.getClientIPAddress();
                    if (ciaadr != 0) {
                        Ip4Address clientIaddr = Ip4Address.valueOf(ciaadr);
                        if (dhcpServerStore.assignIP(hostId, clientIaddr, leaseTime, false, Lists.newArrayList())) {
                            outgoingPacketType = DHCPPacketType.DHCPACK;
                            // discoverHost(context, clientIaddr);
                        } else if (packet.getEtherType() == Ethernet.TYPE_IPV4 &&
                                ((IPv4) packet.getPayload()).getDestinationAddress() == myIP.toInt()) {
                            outgoingPacketType = DHCPPacketType.DHCPNAK;
                        } else {
                            return;
                        }
                        Ethernet ethReply = buildReply(packet, clientIaddr, (byte) outgoingPacketType.getValue());
                        sendReply(context, ethReply);
                    }
                }
            } else if (incomingPacketType.getValue() == DHCPPacketType.DHCPRELEASE.getValue()) {
                Ip4Address ip4Address = dhcpServerStore.releaseIP(hostId);
                if (ip4Address != null) {
                    hostProviderService.removeIpFromHost(hostId, ip4Address);
                }
            }
        }
    }


    private void processArpPacket(PacketContext context, Ethernet packet) {

        ARP arpPacket = (ARP) packet.getPayload();

        ARP arpReply = (ARP) arpPacket.clone();
        arpReply.setOpCode(ARP.OP_REPLY);

        arpReply.setTargetProtocolAddress(arpPacket.getSenderProtocolAddress());
        arpReply.setTargetHardwareAddress(arpPacket.getSenderHardwareAddress());
        arpReply.setSenderProtocolAddress(arpPacket.getTargetProtocolAddress());
        arpReply.setSenderHardwareAddress(myMAC.toBytes());

        // Ethernet Frame.
        Ethernet ethReply = new Ethernet();
        ethReply.setSourceMACAddress(myMAC);
        ethReply.setDestinationMACAddress(packet.getSourceMAC());
        ethReply.setEtherType(Ethernet.TYPE_ARP);
        ethReply.setVlanID(packet.getVlanID());

        ethReply.setPayload(arpReply);
        sendReply(context, ethReply);
    }




        private void discoverHost(PacketContext context, Ip4Address ipAssigned) {
            Ethernet packet = context.inPacket().parsed();
            MacAddress mac = packet.getSourceMAC();
            VlanId vlanId = VlanId.vlanId(packet.getVlanID());
            HostLocation hostLocation = new HostLocation(context.inPacket().receivedFrom(), 0);

            Set<IpAddress> ips = new HashSet<>();
            ips.add(ipAssigned);

            HostId hostId = HostId.hostId(mac, vlanId);
            DefaultHostDescription desc = new DefaultHostDescription(mac, vlanId, hostLocation, ips);
            hostProviderService.hostDetected(hostId, desc);
        }

*/

}
