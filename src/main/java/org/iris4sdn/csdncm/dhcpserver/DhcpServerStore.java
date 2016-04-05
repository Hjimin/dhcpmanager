package org.iris4sdn.csdncm.dhcpserver;

import org.onlab.packet.Ip4Address;
import org.onosproject.net.HostId;
import org.onosproject.vtnrsc.Subnet;


/**
 * DHCPStore Interface.
 */
public interface DhcpServerStore {
    Ip4Address suggestIP(HostId hostId, Ip4Address requestedIP,Subnet subnet);
    Ip4Address releaseIP(HostId hostId);
}
