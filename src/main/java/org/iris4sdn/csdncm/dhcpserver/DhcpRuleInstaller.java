package org.iris4sdn.csdncm.dhcpserver;

import org.onlab.osgi.DefaultServiceDirectory;
import org.onlab.osgi.ServiceDirectory;
import org.onlab.packet.*;
import org.onosproject.core.ApplicationId;
import org.onosproject.net.DeviceId;
import org.onosproject.net.driver.DriverService;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.flowobjective.Objective;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by gurum on 16. 3. 24.
 */
public class DhcpRuleInstaller {
    private final Logger log = getLogger(getClass());

    private final FlowObjectiveService flowObjectiveService;
    private final DriverService driverService;
    private final ApplicationId appId;

    private static final int PREFIX_LENGTH = 32;

    private static final short ARP_RESPONSE = 0x2;


    //don't know
    private static final int ARP_CLASSIFIER_PRIORITY = 55000;
    private static final int DEFAULT_PRIORITY = 50000;
    private static final int DHCP_PRIORITY = 65000;

    // Default NAT time out is 3 minutes.
    private static final int DEFAULT_TIMEOUT = 30;

    private DhcpRuleInstaller(ApplicationId appId) {
        this.appId = checkNotNull(appId, "ApplicationId can not be null");
        ServiceDirectory serviceDirectory = new DefaultServiceDirectory();
        this.flowObjectiveService = serviceDirectory.get(FlowObjectiveService.class);
        this.driverService = serviceDirectory.get(DriverService.class);
    }

    public static DhcpRuleInstaller ruleInstaller(ApplicationId appId) {
        return new DhcpRuleInstaller(appId);
    }

    public void programDhcp(DeviceId deviceId, IpAddress dstIpAddress,
                            IpAddress srcIpAddress, MacAddress dstMac, MacAddress srcVmMac,
                            Objective.Operation type){
        log.info("Program DHCP rules");
        TrafficSelector selector = DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4)
                .matchEthDst(dstMac)
                .matchEthSrc(srcVmMac)
                .matchIPSrc(IpPrefix.valueOf(srcIpAddress, PREFIX_LENGTH))
                .matchIPDst(IpPrefix.valueOf(dstIpAddress, PREFIX_LENGTH))
                .matchIPProtocol(IPv4.PROTOCOL_UDP)
                .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder().punt()
                .build();

        ForwardingObjective.Builder objective = DefaultForwardingObjective
                .builder().withTreatment(treatment).withSelector(selector)
                .fromApp(appId).withFlag(ForwardingObjective.Flag.SPECIFIC)
                .withPriority(DHCP_PRIORITY);

        if (type.equals(Objective.Operation.ADD)) {
            flowObjectiveService.forward(deviceId, objective.add());
        } else {
            flowObjectiveService.forward(deviceId, objective.remove());
        }
    }

    public void programGate(DeviceId deviceId,
                                IpAddress srcipAddress, Objective.Operation type) {
        log.info("Program gate rules");

        TrafficSelector selector = DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4)
                .matchIPProtocol(IPv4.PROTOCOL_TCP)
                .matchIPSrc(IpPrefix.valueOf(srcipAddress, PREFIX_LENGTH))
                .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder().punt()
                .build();

        ForwardingObjective.Builder objective = DefaultForwardingObjective
                .builder().withTreatment(treatment).withSelector(selector)
                .fromApp(appId).withFlag(ForwardingObjective.Flag.SPECIFIC)
                .withPriority(DHCP_PRIORITY);

        if (type.equals(Objective.Operation.ADD)) {
            flowObjectiveService.forward(deviceId, objective.add());
        } else {
            flowObjectiveService.forward(deviceId, objective.remove());
        }
    }
}

