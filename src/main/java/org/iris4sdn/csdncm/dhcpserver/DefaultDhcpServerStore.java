package org.iris4sdn.csdncm.dhcpserver;

import org.apache.felix.scr.annotations.*;
import org.onlab.packet.Ip4Address;
import org.onlab.util.KryoNamespace;
import org.onosproject.net.HostId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.ConsistentMap;
import org.onosproject.store.service.DistributedSet;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.vtnrsc.Subnet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

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

    private DistributedSet<Ip4Address> freeIPPool;

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
        freeIPPool = storageService.<Ip4Address>setBuilder()
                .withName("onos-dhcp-freeIP")
                .withSerializer(Serializer.using(KryoNamespaces.API))
                .build();

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        log.info("Stopped");
    }

    @Override
    public Ip4Address suggestIP(HostId hostId, Ip4Address requestedIP, Subnet subnet) {
        log.info("requested IP {}", requestedIP);
        if (requestedIP.toInt() != 0) {
            log.info("allcation Map doesn't containKey");
            log.info("Client has requested an IP");
            return requestedIP;
        }
        return null;
    }


    @Override
    public Ip4Address releaseIP(HostId hostId) {
        if (allocationMap.containsKey(hostId)) {
            IpAssignmentHandler newAssignment = IpAssignmentHandler.builder(allocationMap.get(hostId).value())
                    .assignmentStatus(IpAssignmentHandler.AssignmentStatus.Option_Expired)
                    .build();
            Ip4Address freeIP = newAssignment.ipAddress();
            allocationMap.put(hostId, newAssignment);
            return freeIP;
        }
        return null;
    }

}
