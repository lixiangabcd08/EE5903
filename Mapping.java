package EE5903;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import org.cloudbus.cloudsim.Host;

public class Mapping {
	private List<Host> hostList;
	private int[] hostPrimary;

	private ArrayList<Double> vmFinishTime = new ArrayList<Double>();


	private HashMap<Integer,Integer> cloudletDeadline = new HashMap<Integer,Integer>();

	public Mapping() {

	}
	public void setHostList (List<Host> hostList) {
		this.hostList = hostList;
		hostPrimary = new int[hostList.size()]; //default 0
	}
	public void addPrimary (int hostId) {
		hostPrimary[hostId]++;
	}
	public List<Host> getSortedHost() {
		List<Host> sortedHosts = this.hostList;
		Collections.sort(sortedHosts, new Comparator<Host>() {
			@Override
			public int compare(Host h1, Host h2) {
				return hostPrimary[h1.getId()]-hostPrimary[h2.getId()];
			}
		});
		return sortedHosts;
	}

	public void setVmFinishTime(int vmId, double finishTime) {
		vmFinishTime.add(vmId, finishTime);
	}

	public double getFinishTime(int vmId) {
		return vmFinishTime.get(vmId);
	}

	public void setCloudletDeadline(int cloudletId, int deadline) {
		cloudletDeadline.put(cloudletId, deadline);

	}

	public int getCloudletDeadline(int cloudletId) {
		return cloudletDeadline.get(cloudletId);
	}
}