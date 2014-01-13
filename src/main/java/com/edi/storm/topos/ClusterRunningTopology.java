package com.edi.storm.topos;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;

/**
 * A sample topology that will only running in the cluster.
 * 
 * @author Edison Xu
 * 
 *         Jan 13, 2014
 */
public class ClusterRunningTopology extends ExclaimBasicTopo {

	public static void main(String[] args) throws Exception {

		String topoName = "test";
		
		ClusterRunningTopology topo = new ClusterRunningTopology();
		Config conf = new Config();
		conf.setDebug(false);

		conf.setNumWorkers(3);

		StormSubmitter.submitTopology(topoName, conf, topo.buildTopology());
	}
}
