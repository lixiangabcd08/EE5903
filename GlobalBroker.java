package EE5903;

import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public class GlobalBroker extends SimEntity {
		private static final int CREATE_BROKER = 0;
		private List<Vm> vmList;
		private List<Cloudlet> cloudletList;
		private List<Host> hostList;
		private FESTALDatacenterBroker broker;
		private int trigger = 0;
		private LinkedList<FESTALDatacenterBroker> brokerList = new LinkedList<FESTALDatacenterBroker> ();
		private Mapping m = new Mapping();
		private Helper helper = new Helper();
		
		public GlobalBroker(String name) {
			super(name);
		}

		@Override
		public void processEvent(SimEvent ev) {
			switch (ev.getTag()) {
			case CREATE_BROKER:
				trigger += 1;

				setBroker(helper.createBroker(super.getName()+'_'+trigger,m));

				//Create VMs and Cloudlets and send them to broker
				setVmList(helper.createVM(getBroker().getId(), 5, trigger*100)); //creating 5 vms
				setCloudletList(helper.createCloudlet(getBroker().getId(), 10, trigger*100)); // creating 10 cloudlets

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

		public void setMapping(Mapping m) {
			this.m = m;
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

		public FESTALDatacenterBroker getBroker() {
			return broker;
		}

		protected void setBroker(FESTALDatacenterBroker broker) {
			this.broker = broker;
		}

		protected void addBroker(FESTALDatacenterBroker broker) {
			this.brokerList.add(broker);
		}

		public List<FESTALDatacenterBroker> getBrokers() {
			return brokerList;
		}
	}
