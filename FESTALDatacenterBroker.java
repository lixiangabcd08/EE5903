package EE5903;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

public class FestalDatacenterBroker extends DatacenterBroker {
	private Mapping m = new Mapping(); 
	private Helper helper = new Helper();
	private List<Host> hostList;
	int backupCloudletIdGap = 10000000;
	int childCloudletIdGap = 5000000;
	protected List<? extends Cloudlet> cloudletList;
	private HashMap<Integer, Cloudlet> childCloudletMap = new HashMap<Integer, Cloudlet>();
//	String algo = "FESTAL";
	String algo = "RAPA";
	private int bandWidth = 100;
	WeibullDistribution weibull= new WeibullDistribution(1,1);
	private int hostLifeSpan = 100000000;

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

		List<Cloudlet> childCloudlet = helper.createCloudlet(getId(), 100, childCloudletIdGap);
		for(Cloudlet cloudlet : childCloudlet) {
			m.setCloudletDeadline(cloudlet.getCloudletId(), Integer.MAX_VALUE); //infinite deadline
			childCloudletMap.put(cloudlet.getCloudletId(), cloudlet);
		}


		submitCloudletList(cloudletList);
		if(algo=="RAPA")
			submitCloudletList(childCloudlet);
	}


	@Override
	protected void submitCloudlets() {
		List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();

		if(algo=="FESTAL") {
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
		}
		else {
			double vmMip = vmList.get(0).getMips();
			HashMap<Integer, Double> rankMap = new HashMap<Integer, Double>();
			for (Cloudlet cloudlet : getCloudletList()) {
				if (cloudlet.getCloudletId()>=childCloudletIdGap) { // child
					double rank = cloudlet.getCloudletLength()/vmMip;
					rankMap.put(cloudlet.getCloudletId(),rank);
				} else { // parent
					double w = cloudlet.getCloudletFileSize()/bandWidth;
					double childRank = childCloudletMap.get(cloudlet.getCloudletId()+childCloudletIdGap).getCloudletLength()/vmMip;
					double rank = cloudlet.getCloudletLength()/vmMip + childRank + w;
					rankMap.put(cloudlet.getCloudletId(),rank);
				}
			}
			List<Cloudlet> sortedCloudletList = getCloudletList();
			// non-increasing order
			Collections.sort(sortedCloudletList, new Comparator<Cloudlet>() {
				@Override
				public int compare(Cloudlet c1, Cloudlet c2) {
					if(rankMap.get(c1.getCloudletId())<rankMap.get(c2.getCloudletId()))
						return 1;
					else
						return -1;
				}
			});
			List<Host> sortedHostList = getSortedHostByFailure();
			for (Cloudlet cloudlet : getCloudletList()) {
				boolean find = false;
				double eft = Integer.MAX_VALUE;
				Vm v = null;

				for (Vm vm : vmList) {
					double eftVm = m.getFinishTime(vm.getId())+cloudlet.getCloudletLength()/vm.getMips();
					if (eftVm < m.getCloudletDeadline(cloudlet.getCloudletId())) {
						find = true;
						if (eftVm < eft) {
							eft = eftVm;
							v = vm;
						}
					}
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
			
		}



		// remove submitted cloudlets from waiting list
		getCloudletList().removeAll(successfullySubmitted);
	}

	
	private List<Host> getSortedHostByFailure() {
		List<Host> sortedHostList = (List<Host>) hostList;
		double t = CloudSim.clock();
		Collections.sort(sortedHostList, new Comparator<Host>() {
			@Override
			public int compare(Host c1, Host c2) {
				if(getFailureRate(t,c1)>getFailureRate(t,c2))
					return 1;
				else
					return -1;
			}
		});		
		return sortedHostList;
	}
	
	private double getFailureRate(double time, Host h) {
		return weibull.density(time/hostLifeSpan);
	}
	
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
		case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
			processResourceCharacteristicsRequest(ev);
			break;
			// Resource characteristics answer
		case CloudSimTags.RESOURCE_CHARACTERISTICS:
			processResourceCharacteristics(ev);
			break;
			// VM Creation answer
		case CloudSimTags.VM_CREATE_ACK:
			processVmCreate(ev);
			break;
			// A finished cloudlet returned
		case CloudSimTags.CLOUDLET_RETURN:
			// start the other dependent task
			if (algo=="FESTAL") {
				int parentID = ((Cloudlet) ev.getData()).getCloudletId();
				if(parentID<childCloudletIdGap) { // if it is a parent cloudlet
					Cloudlet childCloudlet = childCloudletMap.get(parentID+childCloudletIdGap);
					submitCloudletList(new LinkedList<Cloudlet>(Arrays.asList(childCloudlet)));

					submitCloudlets();
					System.out.println("This point is done");
					sendNow(2, CloudSimTags.CLOUDLET_SUBMIT, childCloudlet);
				}
			}
			processCloudletReturn(ev);
			break;
			// if the simulation finishes
		case CloudSimTags.END_OF_SIMULATION:
			shutdownEntity();
			break;
			// other unknown tags are processed by this method
		default:
			processOtherEvent(ev);
			break;
		}
	}

	public void setHost(List<Host> hostList) {
		this.hostList = hostList;
		m.setHostList(hostList);
	}

}
