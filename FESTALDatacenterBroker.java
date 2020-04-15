package EE5903;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

public class FestalDatacenterBroker extends DatacenterBroker {
	private Mapping m = new Mapping(); 
	private Helper helper = new Helper();
	private List<Host> hostList;

	// defined in the algorithm
	private double a = 0.2;

	public FestalDatacenterBroker(String name) throws Exception {
		super(name);
	}

	public void setUp() {
		//Fourth step: Create VMs and Cloudlets and send them to broker

		// submit vm list to the broker
		List<Vm> vmList = helper.createVM(getId(), 5, 0);
		for(Vm v : vmList) {
			m.setVmFinishTime(v.getId(), 0);
		}
		submitVmList(vmList);

		// submit cloudlet list to the broker
		List<Cloudlet> cloudletList = helper.createCloudlet(getId(), 100, 0);
		for(Cloudlet cloudlet : cloudletList) {
			m.setCloudletDeadline(cloudlet.getCloudletId(), Integer.MAX_VALUE); //infinite deadline
		}
		submitCloudletList(cloudletList);
	}


	@Override
	protected void submitCloudlets() {
		List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();

		for (Cloudlet cloudlet : getCloudletList()) {
			// Sort Ha in an increasing order by the count of scheduled primaries
			List<Host> hosts = m.getSortedHost();
			int size = hosts.size();
			boolean find = false;
			double eft = Integer.MAX_VALUE;
			Vm v = null;
			for (int i=0; i<1/a && !find; i++) {
				for (int j=(int) (size*a*i); j<size*a*(i+1); j++) {
					for (Vm vm : hosts.get(j).getVmList()) {
						double eftVm = m.getFinishTime(vm.getId())+cloudlet.getCloudletLength()/vm.getMips();
						if (eftVm < m.getCloudletDeadline(cloudlet.getCloudletId())) {
							find = true;
							if (eftVm < eft) {
								eft = eftVm;
								v = vm;
							}
						}
					}
				}
				if (find) 
					break;
			}
			if (find) {
				cloudlet.setVmId(v.getId());
				sendNow(getVmsToDatacentersMap().get(v.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
				cloudletsSubmitted++;
				getCloudletSubmittedList().add(cloudlet);
				successfullySubmitted.add(cloudlet);
				m.addPrimary(v.getHost().getId());
				m.setVmFinishTime(v.getId(), Math.max(m.getFinishTime(v.getId()),CloudSim.clock())+cloudlet.getCloudletLength()/v.getMips());
				if (!Log.isDisabled()) {
					Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending cloudlet ",
							cloudlet.getCloudletId(), " to VM #", v.getId());
				}
			}
		}

		// remove submitted cloudlets from waiting list
		getCloudletList().removeAll(successfullySubmitted);
	}

	public void setHost(List<Host> hostList) {
		this.hostList = hostList;
		m.setHostList(hostList);
	}

}
