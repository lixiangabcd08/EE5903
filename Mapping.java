package EE5903;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Host;

public class Mapping {
		private List<Host> hostList;
		private int[] hostPrimary;
		
		private Map<Integer, Double> vmFinishTime;
		
		private Map<Integer, Integer> cloudletDeadline;
		
		public Mapping() {
			
		}
		public void setHostList (List<Host> hostList) {
			this.hostList = hostList;
			hostPrimary = new int[hostList.size()];
		}
		private void addPrimary (int hostId) {
			hostPrimary[hostId]++;
		}
		public List<Host> getSortedHost() {
			List<Host> sortedHosts = hostList;
			Collections.sort(sortedHosts, new Comparator<Host>() {
				  @Override
				  public int compare(Host h1, Host h2) {
				    if(hostPrimary[h1.getId()]>(hostPrimary[h2.getId()]))
				    	return 1;
				    else
				    	return 0;
				  }
				});
			return sortedHosts;
		}
		
		public void setVmFinishTime(int vmId, double finishTime) {
			vmFinishTime.put(vmId, finishTime);
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