package org.iris4sdn.csdncm.dhcpserver;

import org.onlab.packet.Ip4Address;
import org.onlab.packet.MacAddress;
import org.onosproject.net.HostId;
import org.onosproject.vtnrsc.Subnet;
import org.onosproject.vtnrsc.SubnetId;

import java.util.List;
import java.util.Map;


/**
 * DHCPStore Interface.
 */
public interface DhcpServerStore {

    /**
     * Appends all the IPs in a given range to the free pool of IPs.
     *
     * @param startIP Start IP for the range
     * @param endIP End IP for the range
     */
    void populateIPPoolfromRange(SubnetId subnetId, Ip4Address startIP, Ip4Address endIP);

    /**
     * Returns an IP Address for a Mac ID, in response to a DHCP DISCOVER message.
     *
     * @param hostId Host ID of the client requesting an IP
     * @param requestedIP requested IP address
     * @return IP address assigned to the Mac ID
     */
    Ip4Address suggestIP(HostId hostId, Ip4Address requestedIP,Subnet subnet);


    /**
     * Assigns the requested IP to the Mac ID, in response to a DHCP REQUEST message.
     *
     * @param hostId Host Id of the client requesting an IP
     * @param ipAddr IP Address being requested
     * @param leaseTime Lease time offered by the server for this mapping
     * @param rangeNotEnforced true if rangeNotEnforced was set
     * @param addressList subnetMask, DHCP/Router/DNS IP Addresses if rangeNotEnforced was set
     * @return returns true if the assignment was successful, false otherwise
     */
    boolean assignIP(HostId hostId, Ip4Address ipAddr, int leaseTime, boolean rangeNotEnforced,
                     List<Ip4Address> addressList, Subnet subnet);


    /**
     * Sets the default time for which suggested IP mappings are valid.
     *
     * @param timeInSeconds default time for IP mappings to be valid
     */
    void setDefaultTimeoutForPurge(int timeInSeconds);

    /**
     * Releases the IP assigned to a Mac ID into the free pool.
     *
     * @param hostId the host ID for which the mapping needs to be changed
     * @return released ip
     */
    Ip4Address releaseIP(HostId hostId);

    /**
     * Returns a collection of all the MacAddress to IPAddress mapping assigned to the hosts.
     *
     * @return the collection of the mappings
     */
    Map<HostId, IpAssignmentHandler> listAssignedMapping();

    /**
     * Returns a collection of all the MacAddress to IPAddress mapping.
     *
     * @return the collection of the mappings
     */
    Map<HostId, IpAssignmentHandler> listAllMapping();

    /**
     * Assigns the requested IP to the MAC ID (if available) for an indefinite period of time.
     *
     * @param macID macID of the client
     * @param ipAddr IP Address requested for the client
     * @param rangeNotEnforced true if rangeNotEnforced was set
     * @param addressList subnetMask, DHCP/Router/DNS IP Addresses rangeNotEnforced was set
     * @return true if the mapping was successfully registered, false otherwise
     */
    boolean assignStaticIP(MacAddress macID, Ip4Address ipAddr, boolean rangeNotEnforced, List<Ip4Address> addressList, Subnet subnet);

    /**
     * Removes a static IP mapping associated with the given MAC ID from the DHCP Server.
     *
     * @param macID macID of the client
     * @return true if the mapping was successfully registered, false otherwise
     */
    boolean removeStaticIP(MacAddress macID);

    /**
     * Returns the list of all the available IPs with the server.
     *
     * @return list of available IPs
     */
    //Iterable<Ip4Address> getAvailableIPs();

    /**
     *
     *
     * @param hostId
     * @return
     */
    IpAssignmentHandler getIpAssignmentFromAllocationMap(HostId hostId);

}
