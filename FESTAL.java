package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/*
 * Title:        EE5903
 * Description:  CloudSim (Cloud Simulation) for FESTAL algorithm
 *
 * Author: Li Xiang A0115448E
 */

 import org.cloudbus.cloudsim.Cloudlet;
 import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
 import org.cloudbus.cloudsim.Datacenter;
 import org.cloudbus.cloudsim.DatacenterBroker;
 import org.cloudbus.cloudsim.DatacenterCharacteristics;
 import org.cloudbus.cloudsim.Host;
 import org.cloudbus.cloudsim.Log;
 import org.cloudbus.cloudsim.Pe;
 import org.cloudbus.cloudsim.Storage;
 import org.cloudbus.cloudsim.UtilizationModel;
 import org.cloudbus.cloudsim.UtilizationModelFull;
 import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
 import org.cloudbus.cloudsim.VmSchedulerTimeShared;
 import org.cloudbus.cloudsim.core.CloudSim;
 import org.cloudbus.cloudsim.core.SimEntity;
 import org.cloudbus.cloudsim.core.SimEvent;
 import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
 import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
 import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class FESTAL {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList0;
	private static List<Cloudlet> cloudletList1;

	/** The vmList. */
	private static List<Vm> vmList0;
	private static List<Vm> vmList1;

	public static void main(String [] args) {
		try {
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
 			boolean trace_flag = false; // trace events
			CloudSim.init(num_user, calendar, trace_flag);

			GlobalBroker globalBroker = new GlobalBroker("GlobalBroker");

			// Second step: Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			//Third step: Create Broker
			DatacenterBroker broker = createBroker("Broker_0");
			int brokerId = broker.getId();

			//Fourth step: Create VMs and Cloudlets and send them to broker
			vmList0 = createVM(brokerId, 5, 0); //creating 5 vms
			cloudletList0 = createCloudlet(brokerId, 10, 0); // creating 10 cloudlets

			broker.submitVmList(vmList0);
			broker.submitCloudletList(cloudletList0);

			// Fifth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
//			newList.addAll(globalBroker.getBroker().getCloudletReceivedList());
			List<DatacenterBroker> borkerList= globalBroker.getBrokers();
			for (int i=0; i<borkerList.size(); i++) {
				newList.addAll(borkerList.get(i).getCloudletReceivedList());
			}

			CloudSim.stopSimulation();

			printCloudletList(newList);

			Log.printLine("CloudSimExample8 finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	public static class PrimaryVmAllocation extends VmAllocationPolicy {
		//To track the Host for each Vm. The string is the unique Vm identifier, composed by its id and its userId
		private Map<String, Host> vmTable;
		
		//To track the primary Vm for each Host. The string is the unique Vm identifier, composed by its id and its userId
		private Map<Integer, List<Host>> hostTable;
		
		private LinkedList<Integer> primaryNumber = new LinkedList<Integer> ();
		
		// defined in the algorithm
		private double a = 0.2;
		
		public PrimaryVmAllocation(List<? extends Host> list) {
			super(list);
			vmTable = new HashMap<>();
		}

		public Host getHost(Vm vm) {
			// We must recover the Host which hosting Vm
			return this.vmTable.get(vm.getUid());
		}

		public Host getHost(int vmId, int userId) {
			// We must recover the Host which hosting Vm
			return this.vmTable.get(Vm.getUid(userId, vmId));
		}
		
		public boolean allocateHostForVm(Vm vm, Host host) {
			Collections.sort(primaryNumber);
			for(int i=0; i<primaryNumber.size()/a; i++) {
				
			}
			
			if (host.vmCreate(vm)) {
				//the host is appropriate, we track it
				vmTable.put(vm.getUid(), host);
				return true;
			}
			return false;
		}

		public boolean allocateHostForVm(Vm vm) {
			//First fit algorithm, run on the first suitable node
			for (Host h : getHostList()) {
				if (h.vmCreate(vm)) {
					//track the host
					vmTable.put(vm.getUid(), h);
					return true;
				}
			}
			return false;
		}

	    public void deallocateHostForVm(Vm vm, Host host) {
	        vmTable.remove(vm.getUid());
	        host.vmDestroy(vm);
	    }
	    
	    @Override
	    public void deallocateHostForVm(Vm v) {
	        //get the host and remove the vm
	    	//TODO: why don't need to remove host from the table??
	        vmTable.get(v.getUid()).vmDestroy(v);
	    }
		
	    public Object optimizeAllocation() {
	        return null;
	    }
	    
	    @Override
	    public List<Map<String, Object>> optimizeAllocation(List<? extends Vm> arg0) {
	        //Static scheduling, no migration, return null;
	        return null;
	    }
	}

	
	private static DatacenterBroker createBroker(String name) {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker(name);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	public static class GlobalBroker extends SimEntity {
		private static final int CREATE_BROKER = 0;
		private List<Vm> vmList;
		private List<Cloudlet> cloudletList;
		private DatacenterBroker broker;
		private int trigger = 0;
		private LinkedList<DatacenterBroker> brokerList = new LinkedList<DatacenterBroker> ();

		public GlobalBroker(String name) {
			super(name);
		}

		@Override
		public void processEvent(SimEvent ev) {
			switch (ev.getTag()) {
			case CREATE_BROKER:
				trigger += 1;

				setBroker(createBroker(super.getName()+'_'+trigger));

				//Create VMs and Cloudlets and send them to broker
				setVmList(createVM(getBroker().getId(), 5, trigger*100)); //creating 5 vms
				setCloudletList(createCloudlet(getBroker().getId(), 10, trigger*100)); // creating 10 cloudlets

				broker.submitVmList(getVmList());
				broker.submitCloudletList(getCloudletList());
				addBroker(broker);

				CloudSim.resumeSimulation();

				break;

			default:
				Log.printLine(getName() + ": unknown event type");
				break;
			}
		}

		@Override
		public void startEntity() {
			Log.printLine(super.getName()+" is starting...");
			schedule(getId(), 200, CREATE_BROKER);
			schedule(getId(), 400, CREATE_BROKER);
			schedule(getId(), 600, CREATE_BROKER);
		}

		@Override
		public void shutdownEntity() {
		}

		public List<Vm> getVmList() {
			return vmList;
		}

		protected void setVmList(List<Vm> vmList) {
			this.vmList = vmList;
		}

		public List<Cloudlet> getCloudletList() {
			return cloudletList;
		}

		protected void setCloudletList(List<Cloudlet> cloudletList) {
			this.cloudletList = cloudletList;
		}

		public DatacenterBroker getBroker() {
			return broker;
		}

		protected void setBroker(DatacenterBroker broker) {
			this.broker = broker;
		}

		protected void addBroker(DatacenterBroker broker) {
			this.brokerList.add(broker);
		}

		public List<DatacenterBroker> getBrokers() {
			return brokerList;
		}
	}

	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = createHostList(1000);

		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		int schedulingInterval = 0;
		PrimaryVmAllocation vmAllocationPolicy = new PrimaryVmAllocation(hostList);
		try {
			datacenter = new Datacenter(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// TODO:datacenter.setDisableMigrations(false);
		return datacenter;
	}

	private static List<Host> createHostList(int hostsNumber){
		List<Host> hostList = new ArrayList<Host>();
		for (int i=0; i<hostsNumber; i++) {
			// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
			//    create a list to store these PEs before creating
			//    a Machine.
			List<Pe> peList = new ArrayList<Pe>();

			//TODO: try different MIPS
			int mips = 1000;

			// 3. Create PEs and add these into the list.
			//for a quad-core machine, a list of 4 PEs is required:
			peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
			peList.add(new Pe(1, new PeProvisionerSimple(mips)));
			peList.add(new Pe(2, new PeProvisionerSimple(mips)));
			peList.add(new Pe(3, new PeProvisionerSimple(mips)));

			//4. Create Hosts with its id and list of PEs and add them to the list of machines
			int hostId=0;
			int ram = 16384; //host memory (MB)
			long storage = 1000000; //host storage
			int bw = 10000;

			hostList.add(
	    			new Host(
	    				hostId,
	    				new RamProvisionerSimple(ram),
	    				new BwProvisionerSimple(bw),
	    				storage,
	    				peList,
	    				new VmSchedulerTimeShared(peList)
	    			)
	    		); // This is our first machine
		}
		return hostList;
	}

	private static List<Vm> createVM(int userId, int vms, int idShift) {
		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<Vm> list = new LinkedList<Vm>();

		//VM Parameters
		long size = 10000; //image size (MB)
		int ram = 512; //vm memory (MB)
		int mips = 250;
		long bw = 1000;
		int pesNumber = 1; //number of cpus
		String vmm = "Xen"; //VMM name

		//create VMs
		Vm[] vm = new Vm[vms];

		for(int i=0;i<vms;i++){
			vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			list.add(vm[i]);
		}

		return list;
	}

	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		// Creates a container to store Cloudlets
		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

		//cloudlet parameters
		long length = 40000;
		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			// setting the owner of these Cloudlets
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
		}

		return list;
	}

	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) +
						indent + indent + dft.format(cloudlet.getExecStartTime())+ indent + indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}

	}

}
