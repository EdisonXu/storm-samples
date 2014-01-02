package com.edi.storm.spouts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.edi.storm.util.PrintHelper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * A spout generates random stuff.
 * 
 * @author Edison Xu
 *
 * Dec 30, 2013
 */
public class RandomSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private Random rand;
	
	private AtomicInteger counter;
	
	private static String[] sentences = new String[] {"edi:I'm happy", "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous"};
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.rand = new Random();
		counter = new AtomicInteger();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(5000);
		String toSay = sentences[rand.nextInt(sentences.length)];
		int msgId = this.counter.getAndIncrement();
		toSay = "["+ msgId + "]"+ toSay;
		PrintHelper.print("Send " + toSay );
		
		this.collector.emit(new Values(toSay), msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
    public void ack(Object msgId) {
		PrintHelper.print("ack " + msgId);
    }

    @Override
    public void fail(Object msgId) {
    	PrintHelper.print("fail " + msgId);
    }

}
