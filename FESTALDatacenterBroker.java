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

public class FESTALDatacenterBroker extends DatacenterBroker {
    	private Mapping m; 
    	
		// defined in the algorithm
		private double a = 0.2;
		
        public FESTALDatacenterBroker(String name, Mapping m) throws Exception {
			super(name);
			this.m = m;
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
				}
    		}
			
    		int vmIndex = 4;
    		List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();
    		for (Cloudlet cloudlet : getCloudletList()) {
    			Vm vm;
    			// if user didn't bind this cloudlet and it has not been executed yet
    			if (cloudlet.getVmId() == -1) {
    				vm = getVmsCreatedList().get(vmIndex);
    			} else { // submit to the specific vm
    				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
    				if (vm == null) { // vm was not created
    					if(!Log.isDisabled()) {
    					    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of cloudlet ",
    							cloudlet.getCloudletId(), ": bount VM not available");
    					}
    					continue;
    				}
    			}

    			if (!Log.isDisabled()) {
    			    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending cloudlet ",
    					cloudlet.getCloudletId(), " to VM #", vm.getId());
    			}

    			cloudlet.setVmId(vm.getId());
    			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
    			cloudletsSubmitted++;
    			vmIndex = (vmIndex - 1 + getVmsCreatedList().size()) % getVmsCreatedList().size();
    			getCloudletSubmittedList().add(cloudlet);
    			successfullySubmitted.add(cloudlet);
    			Log.printConcatLine("Cloudlet finishing time ",cloudlet.getFinishTime());
    		}

    		// remove submitted cloudlets from waiting list
    		getCloudletList().removeAll(successfullySubmitted);
    	}
    }
