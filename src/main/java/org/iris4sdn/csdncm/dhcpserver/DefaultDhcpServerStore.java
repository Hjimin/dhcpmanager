package org.iris4sdn.csdncm.dhcpserver;

import org.apache.felix.scr.annotations.*;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.MacAddress;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Tools;
import org.onosproject.net.HostId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.*;
import org.onosproject.vtnrsc.Subnet;
import org.onosproject.vtnrsc.SubnetId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by gurum on 16. 3. 24.
 */

@Component(immediate = true)
@Service
public class DefaultDhcpServerStore implements DhcpServerStore{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    private ConsistentMap<HostId, IpAssignmentHandler> allocationMap;

    //private DistributedSet<Ip4Address> freeIPPool;
    private ConsistentMap<SubnetId, Set<Ip4Address>> subnetIPMap;

    private static Ip4Address startIPRange;

    private static Ip4Address endIPRange;

    // Hardcoded values are default values.

    private static int timeoutForPendingAssignments = 60;
    private static final int MAX_RETRIES = 3;
    private static final int MAX_BACKOFF = 10;

    @Activate
    protected void activate() {
        allocationMap = storageService.<HostId, IpAssignmentHandler>consistentMapBuilder()
                .withName("onos-dhcp-assignedIP")
                .withSerializer(Serializer.using(
                        new KryoNamespace.Builder()
                                .register(KryoNamespaces.API)
                                .register(IpAssignmentHandler.class,
                                        IpAssignmentHandler.AssignmentStatus.class,
                                        Date.class,
                                        long.class,
                                        Ip4Address.class)
                                .build()))
                .build();
        subnetIPMap = storageService.<SubnetId, Set<Ip4Address>>consistentMapBuilder()
                .withName("onos-dhcp-subnet")
                .withSerializer(Serializer.using(
                        new KryoNamespace.Builder()
                                .register(KryoNamespaces.API)
                                .register(SubnetId.class,
                                        Ip4Address.class)
                                .build()))
                .build();
//        freeIPPool = storageService.<Ip4Address>setBuilder()
//                .withName("onos-dhcp-freeIP")
//                .withSerializer(Serializer.using(KryoNamespaces.API))
//                .build();

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        log.info("Stopped");
    }

    @Override
    public Ip4Address suggestIP(HostId hostId, Ip4Address requestedIP, Subnet subnet) {
        log.info("suggest Ip");
        log.info("Host ID {}", hostId);
        log.info("requested IP {}", requestedIP);
        IpAssignmentHandler assignmentInfo;
        Set freeIPPool = (Set)subnetIPMap.get(subnet.id()).value();
        if(allocationMap.isEmpty()){
            log.info("yesWWWWWWWWWWWWWWWWWWWWWWWW");
        }

        if (allocationMap.containsKey(hostId)) {
            log.info("allocation Map containsKey");
            assignmentInfo = allocationMap.get(hostId).value();
            IpAssignmentHandler.AssignmentStatus status = assignmentInfo.assignmentStatus();
            Ip4Address ipAddr = assignmentInfo.ipAddress();

            if (assignmentInfo.rangeNotEnforced()) {
                log.info("assignmentInfo");
                return assignmentInfo.ipAddress();
            } else if (status == IpAssignmentHandler.AssignmentStatus.Option_Assigned ||
                    status == IpAssignmentHandler.AssignmentStatus.Option_Requested) {
                log.info("Client has a currently Active Binding");
                // Client has a currently Active Binding.
                if (ipWithinRange(ipAddr)) {
                    return ipAddr;
                }

            } else if (status == IpAssignmentHandler.AssignmentStatus.Option_Expired) {
                log.info("Client has a Released or Expired Binding");
                if (freeIPPool.contains(ipAddr)) {
                    assignmentInfo = IpAssignmentHandler.builder()
                            .ipAddress(ipAddr)
                            .timestamp(new Date())
                            .leasePeriod(timeoutForPendingAssignments)
                            .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Requested)
                            .build();
                    if (freeIPPool.remove(ipAddr)) {
                        allocationMap.put(hostId, assignmentInfo);
                        return ipAddr;
                    }
                }
            }
        } else if (requestedIP.toInt() != 0) {
            log.info("allcation Map doesn't containKey");
            log.info("Client has requested an IP");
            if (freeIPPool.contains(requestedIP)) {
                assignmentInfo = IpAssignmentHandler.builder()
                        .ipAddress(requestedIP)
                        .timestamp(new Date())
                        .leasePeriod(timeoutForPendingAssignments)
                        .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Requested)
                        .build();
                if (freeIPPool.remove(requestedIP)) {
                    allocationMap.put(hostId, assignmentInfo);
                    return requestedIP;
                }
            }
        }

        log.info("NONONNONONNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNn");
        // Allocate a new IP from the server's pool of available IP.
        Ip4Address nextIPAddr = fetchNextIP(freeIPPool);
        if (nextIPAddr != null) {
            assignmentInfo = IpAssignmentHandler.builder()
                    .ipAddress(nextIPAddr)
                    .timestamp(new Date())
                    .leasePeriod(timeoutForPendingAssignments)
                    .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Requested)
                    .build();

            allocationMap.put(hostId, assignmentInfo);
        } else {
            log.info("asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf");
        }
        return nextIPAddr;

    }

    @Override
    public boolean assignIP(HostId hostId, Ip4Address ipAddr, int leaseTime, boolean rangeNotEnforced,
                            List<Ip4Address> addressList, Subnet subnet) {
        log.debug("Assign IP Called w/ Ip4Address: {}, HostId: {}", ipAddr.toString(), hostId.mac().toString());

        Set freeIPPool = (Set)subnetIPMap.get(subnet.id()).value();
        AtomicBoolean assigned = Tools.retryable(() -> {
            AtomicBoolean result = new AtomicBoolean(false);
            allocationMap.compute(
                    hostId,
                    (h, existingAssignment) -> {
                        IpAssignmentHandler assignment = existingAssignment;
                        if (existingAssignment == null) {
                            if (rangeNotEnforced) {
                                assignment = IpAssignmentHandler.builder()
                                        .ipAddress(ipAddr)
                                        .timestamp(new Date())
                                        .leasePeriod(leaseTime)
                                        .rangeNotEnforced(true)
                                        .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_RangeNotEnforced)
                                        .subnetMask((Ip4Address) addressList.toArray()[0])
                                        .dhcpServer((Ip4Address) addressList.toArray()[1])
                                        .routerAddress((Ip4Address) addressList.toArray()[2])
                                        .domainServer((Ip4Address) addressList.toArray()[3])
                                        .build();
                                result.set(true);
                            } else if (freeIPPool.remove(ipAddr)) {
                                assignment = IpAssignmentHandler.builder()
                                        .ipAddress(ipAddr)
                                        .timestamp(new Date())
                                        .leasePeriod(leaseTime)
                                        .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Assigned)
                                        .build();
                                result.set(true);
                            }
                        } else if (Objects.equals(existingAssignment.ipAddress(), ipAddr) &&
                                (existingAssignment.rangeNotEnforced() || ipWithinRange(ipAddr))) {
                            switch (existingAssignment.assignmentStatus()) {
                                case Option_RangeNotEnforced:
                                    assignment = IpAssignmentHandler.builder()
                                            .ipAddress(ipAddr)
                                            .timestamp(new Date())
                                            .leasePeriod(existingAssignment.leasePeriod())
                                            .rangeNotEnforced(true)
                                            .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_RangeNotEnforced)
                                            .subnetMask(existingAssignment.subnetMask())
                                            .dhcpServer(existingAssignment.dhcpServer())
                                            .routerAddress(existingAssignment.routerAddress())
                                            .domainServer(existingAssignment.domainServer())
                                            .build();
                                    result.set(true);
                                    break;
                                case Option_Assigned:
                                case Option_Requested:
                                    assignment = IpAssignmentHandler.builder()
                                            .ipAddress(ipAddr)
                                            .timestamp(new Date())
                                            .leasePeriod(leaseTime)
                                            .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Assigned)
                                            .build();
                                    result.set(true);
                                    break;
                                case Option_Expired:
                                    if (freeIPPool.remove(ipAddr)) {
                                        assignment = IpAssignmentHandler.builder()
                                                .ipAddress(ipAddr)
                                                .timestamp(new Date())
                                                .leasePeriod(leaseTime)
                                                .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Assigned)
                                                .build();
                                        result.set(true);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        return assignment;
                    });
            return result;
        }, ConsistentMapException.class, MAX_RETRIES, MAX_BACKOFF).get();

        return assigned.get();
    }

    @Override
    public Ip4Address releaseIP(HostId hostId) {
        if (allocationMap.containsKey(hostId)) {
            IpAssignmentHandler newAssignment = IpAssignmentHandler.builder(allocationMap.get(hostId).value())
                    .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Expired)
                    .build();
            Ip4Address freeIP = newAssignment.ipAddress();
            allocationMap.put(hostId, newAssignment);
            if (ipWithinRange(freeIP)) {
                //freeIPPool.add(freeIP);
            }
            return freeIP;
        }
        return null;
    }

    @Override
    public void setDefaultTimeoutForPurge(int timeInSeconds) {
        timeoutForPendingAssignments = timeInSeconds;
    }

    @Override
    public Map<HostId, IpAssignmentHandler> listAssignedMapping() {

        Map<HostId, IpAssignmentHandler> validMapping = new HashMap<>();
        IpAssignmentHandler assignment;
        for (Map.Entry<HostId, Versioned<IpAssignmentHandler>> entry: allocationMap.entrySet()) {
            assignment = entry.getValue().value();
            if (assignment.assignmentStatus() == IpAssignmentHandler.AssignmentStatus.Option_Assigned
                    || assignment.assignmentStatus() == IpAssignmentHandler.AssignmentStatus.Option_RangeNotEnforced) {
                validMapping.put(entry.getKey(), assignment);
            }
        }
        return validMapping;
    }

    @Override
    public Map<HostId, IpAssignmentHandler> listAllMapping() {
        Map<HostId, IpAssignmentHandler> validMapping = new HashMap<>();
        for (Map.Entry<HostId, Versioned<IpAssignmentHandler>> entry: allocationMap.entrySet()) {
            validMapping.put(entry.getKey(), entry.getValue().value());
        }
        return validMapping;
    }

    @Override
    public boolean assignStaticIP(MacAddress macID, Ip4Address ipAddr, boolean rangeNotEnforced,
                                  List<Ip4Address> addressList, Subnet subnet) {
        HostId host = HostId.hostId(macID);
        return assignIP(host, ipAddr, -1, rangeNotEnforced, addressList,subnet);
    }

    @Override
    public boolean removeStaticIP(MacAddress macID) {
        HostId host = HostId.hostId(macID);
        if (allocationMap.containsKey(host)) {
            IpAssignmentHandler assignment = allocationMap.get(host).value();

            if (assignment.rangeNotEnforced()) {
                allocationMap.remove(host);
                return true;
            }

            Ip4Address freeIP = assignment.ipAddress();
            if (assignment.leasePeriod() < 0) {
                allocationMap.remove(host);
                if (ipWithinRange(freeIP)) {
                    //freeIPPool.add(freeIP);
                }
                return true;
            }
        }
        return false;
    }

//    @Override
//    public Iterable<Ip4Address> getAvailableIPs() {
//        return ImmutableSet.copyOf(freeIPPool);
//    }


    @Override
    public void populateIPPoolfromRange(SubnetId subnetId,Ip4Address startIP, Ip4Address endIP) {
        // Clear all entries from previous range.
        Set<Ip4Address> freeIPPool = new HashSet<Ip4Address>();
        allocationMap.clear();
        startIPRange = startIP;
        endIPRange = endIP;


        int lastIP = endIP.toInt();
        Ip4Address nextIP;
        for (int loopCounter = startIP.toInt(); loopCounter <= lastIP; loopCounter++) {
        //for (int loopCounter = 0; loopCounter <= 1; loopCounter++) {
            nextIP = Ip4Address.valueOf(loopCounter);
            freeIPPool.add(nextIP);
        }

        subnetIPMap.put(subnetId, freeIPPool);
    }

    @Override
    public IpAssignmentHandler getIpAssignmentFromAllocationMap(HostId hostId) {
        return allocationMap.get(hostId).value();
    }

    /**
     * Fetches the next available IP from the free pool pf IPs.
     *
     * @return the next available IP address
     */
    private Ip4Address fetchNextIP(Set<Ip4Address> freeIPPool) {
        for (Ip4Address freeIP : freeIPPool) {
            log.info("freeIP {}", freeIP);
            if (freeIPPool.remove(freeIP)) {
                return freeIP;
            }
        }
        return null;
    }

    /**
     * Returns true if the given ip is within the range of available IPs.
     *
     * @param ip given ip address
     * @return true if within range, false otherwise
     */
    private boolean ipWithinRange(Ip4Address ip) {
        if ((ip.toInt() >= startIPRange.toInt()) && (ip.toInt() <= endIPRange.toInt())) {
            return true;
        }
        return false;
    }

}
