/**
 * 
 */
package com.edi.storm;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.edi.storm.util.DBManager;

/**
 * @author Edison Xu
 * 
 *         Dec 5, 2013
 */
public class TransactionalDBWriter {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		BatchSpout spout = new BatchSpout(1);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trans-db-writer", conf,
					buildTopology(spout));
			Thread.sleep(1000);
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(spout));
		}
	}

	static class BatchSpout implements IBatchSpout {
		private static final Logger LOGGER = LoggerFactory.getLogger(BatchSpout.class);
		private int batchSize;
		private static final AtomicInteger seq = new AtomicInteger(0);

		public BatchSpout() {
			super();
			this.batchSize = 1;
		}
		
		public BatchSpout(int batchSize) {
			super();
			this.batchSize = batchSize;
		}

		public void open(Map conf, TopologyContext context) {
			
		}

		public void emitBatch(long batchId, TridentCollector collector) {
			TransObj obj = new TransObj(seq.getAndIncrement(), System.currentTimeMillis());
			LOGGER.info("Fire!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			collector.emit(new Values(obj));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void ack(long batchId) {
			// TODO Auto-generated method stub

		}

		public void close() {
			// TODO Auto-generated method stub

		}

		public Map getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		public Fields getOutputFields() {
			return new Fields("obj");
		}

	}

	public static StormTopology buildTopology(BatchSpout spout) {
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("transwrite", spout)
			.each(new Fields("obj"), new WriteDB(), new Fields("after"))
		;
		return topology.build();
	}
	
	static class TransObj implements Serializable{
		private static final long serialVersionUID = -2220707446968927825L;
		private int seq;
		private long initTime;
		public int getSeq() {
			return seq;
		}
		public void setSeq(int seq) {
			this.seq = seq;
		}
		public long getInitTime() {
			return initTime;
		}
		public void setInitTime(long initTime) {
			this.initTime = initTime;
		}
		public TransObj(int seq, long initTime) {
			super();
			this.seq = seq;
			this.initTime = initTime;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (initTime ^ (initTime >>> 32));
			result = prime * result + seq;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TransObj other = (TransObj) obj;
			if (initTime != other.initTime)
				return false;
			if (seq != other.seq)
				return false;
			return true;
		}
	}
	
	private static class WriteDB extends BaseFunction {
		private static  List<TransObj> list = new ArrayList<TransObj>(); 
		private int MAX_SIZE = 10;
		private static final String INSERT_STRING = "INSERT INTO `STORM_TRANS_TEST` (`seq`,`init_time`,`insert_time`,`latency`) values (%d,%d,%d,%d)";
		private static final String P_INSERT_STRING = "INSERT INTO `STORM_TRANS_TEST` (`seq`,`init_time`,`insert_time`,`latency`) values (?,?,?,?)";
		private boolean batch = false;
		private static final Logger LOGGER = LoggerFactory.getLogger(WriteDB.class);
		
        public void execute(TridentTuple tuple, TridentCollector collector) {
        	LOGGER.info("Executed!!!!!!!!!!!!!!!!!!!!!!!!!");
        	if(tuple==null) return;
        	TransObj obj = null;
        	long beginTime = System.currentTimeMillis();
        	if(tuple.get(0)!=null && tuple.get(0) instanceof TransObj)
        	{
        		obj = (TransObj) tuple.get(0);
        		System.out.println("ExeLatency: " + (beginTime -obj.getInitTime()));
        	}
        	if(obj==null) return;
        	if(batch)
        	{
        		synchronized (list) {
            		if(list.size()>=MAX_SIZE)
            		{
            			insertData();
            			list.clear();
            		}else
            		{
            			/*String dml = String.format(INSERT_STRING, obj.getSeq(),obj.getInitTime(),System.currentTimeMillis());
            			list.add(dml);*/
            			list.add(obj);
            		}
    			}
        	}else
        	{
        		insertImmediately(obj);
        	}
        	
        	System.out.println("Latency2: " + (System.currentTimeMillis()-beginTime));
        	System.out.println("TotalLatency: " + (System.currentTimeMillis()-obj.getInitTime()));
            collector.emit(new Values(obj));
        }
        
        private void insertImmediately(TransObj obj)
        {
        	Connection conn = null;
			try {
				conn = DBManager.getConnection();
				/*PreparedStatement prepStatement = conn.prepareStatement(P_INSERT_STRING);
				prepStatement.setInt(1, obj.getSeq());
				prepStatement.setLong(2, obj.getInitTime());
				prepStatement.setLong(3, System.currentTimeMillis());
				prepStatement.setLong(4, (System.currentTimeMillis()-obj.getInitTime()));
				prepStatement.addBatch();
				prepStatement.executeBatch();*/
				
				String dml = String.format(INSERT_STRING, obj.getSeq(),obj.getInitTime(),System.currentTimeMillis(),(System.currentTimeMillis()-obj.getInitTime()));
				Statement stmt = conn.createStatement();
				stmt.addBatch(dml);
				stmt.executeBatch();
	    		System.out.println("============Insert OK==============");
			} catch (SQLException e1) {
				e1.printStackTrace();
			}finally{
				if(conn!=null)
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
			}
        }
        
        private void insertData()
        {
        	Connection conn = null;
			try {
				conn = DBManager.getConnection();
				PreparedStatement prepStatement = conn.prepareStatement(P_INSERT_STRING);
				for(TransObj obj:list)
				{
					/*Statement stmt = conn.createStatement();
					stmt.addBatch(dml);
					stmt.executeBatch();*/
					prepStatement.setInt(1, obj.getSeq());
					prepStatement.setLong(2, obj.getInitTime());
					prepStatement.setLong(3, System.currentTimeMillis());
					prepStatement.setLong(4, (System.currentTimeMillis()-obj.getInitTime()));
					prepStatement.addBatch();
				}
				prepStatement.executeBatch();
	    		System.out.println("============Insert OK==============");
			} catch (SQLException e1) {
				e1.printStackTrace();
			}finally{
				if(conn!=null)
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
			}
        }
    }
}
