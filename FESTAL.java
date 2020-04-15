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
import org.cloudbus.cloudsim.core.CloudSim;

public class FESTAL {

	public static void main(String [] args) {
		try {

			Helper h = new Helper();
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
			boolean trace_flag = false; // trace events
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter = h.createDatacenter("Datacenter_0");

			// Third step: Create Broker
			FestalDatacenterBroker broker = h.createBroker();

			broker.setHost(datacenter.getHostList());
			broker.setUp();

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			//Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			h.printCloudletList(newList);

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}
}
