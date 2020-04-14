/*
 * Title:        EE5903
 * Description:  CloudSim (Cloud Simulation) for FESTAL algorithm
 *
 * Author: Li Xiang A0115448E
 */

package EE5903;

import java.util.Calendar;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

public class FESTAL {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList0;

	/** The vmList. */
	private static List<Vm> vmList0;

	public static void main(String [] args) {
		try {
			Helper helper = new Helper();
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
 			boolean trace_flag = false; // trace events
			CloudSim.init(num_user, calendar, trace_flag);

			GlobalBroker globalBroker = new GlobalBroker("GlobalBroker");

			// Second step: Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
			Datacenter datacenter0 = helper.createDatacenter("Datacenter_0");
			
			Mapping m = new Mapping();
			
			m.setHostList(datacenter0.getHostList());
			
			globalBroker.setMapping(m);

			//Third step: Create Broker
			FESTALDatacenterBroker broker = helper.createBroker("Broker_0",m);
			int brokerId = broker.getId();

			//Fourth step: Create VMs and Cloudlets and send them to broker
			vmList0 = helper.createVM(brokerId, 5, 0); //creating 5 vms
			cloudletList0 = helper.createCloudlet(brokerId, 10, 0); // creating 10 cloudlets

			broker.submitVmList(vmList0);
			broker.submitCloudletList(cloudletList0);

			// Fifth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
//			newList.addAll(globalBroker.getBroker().getCloudletReceivedList());
			List<FESTALDatacenterBroker> borkerList= globalBroker.getBrokers();
			for (int i=0; i<borkerList.size(); i++) {
				newList.addAll(borkerList.get(i).getCloudletReceivedList());
			}

			CloudSim.stopSimulation();

			helper.printCloudletList(newList);

			Log.printLine("CloudSimExample8 finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

}
