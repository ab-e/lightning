/*
 * Copyright (c) PANGAEA - Data Publisher for Earth & Environmental Science
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.pangaea.lightning;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.pangaea.lightning.inputprocessing.DataParser;
import de.pangaea.lightning.inputprocessing.FileIO;
import de.pangaea.lightning.inputprocessing.PropertyReader;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * <p>
 * Title: BasicExample
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Project:
 * </p>
 * <p>
 * Copyright: PANGAEA
 * </p>
 */

public class BasicExample {
	
	private static final String CTD_UUID = "291EC6DA-43F8-46D3-86F5-628FBF955261";
	private static int icounter = 0;
	private static String fileName = "";
	private static InputStream inStream = null;
	private static StringBuffer sBuffer = null;
	private static String cleanString = "";
	private static ValueChecker valueChecker = new ValueChecker();
	private static DOMBuilder domBuilder = new DOMBuilder();
	
	private static Properties properties = new Properties();
	private static ArrayList<String> dataStringList = new ArrayList<>();
	
	public static class NumberSpout extends BaseRichSpout {
		
		
		private FileIO fileIO = null;
		private ArrayList<ArrayList<String>> dataList;
		private String cleanXMLFileName = "";
		
		
		boolean _isDistributed;
		static SpoutOutputCollector _collector;

		public NumberSpout() {
			this(true);
		}

		public NumberSpout(boolean isDistributed) {
			_isDistributed = isDistributed;
		}

		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
			fileIO = new FileIO();
		}

		public void close() {

		}

		public void nextTuple() {
			String templateID = "";
			dataStringList.clear();
			Utils.sleep(100);
			
			Values values = new Values();
			String allValues = new String("");
			
			/**
			 * TEST
			 */
			ArrayList<File> fileList = fileIO.getFiles();
			for(int i = 0; i < fileList.size(); i++){
				
				//setFileName(fileList.get(i).getName());
				fileName = fileList.get(i).getName();
				cleanString = fileIO.checkXML(fileList.get(i));
				
				try {
					dataStringList = DataParser.parseInsertResult(cleanString);
					templateID = dataStringList.remove(0);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (XMLStreamException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String data = fileIO.splitDataString( dataStringList.get(0) );
				data = templateID.concat(",").concat(data);
				
				for(File file: new File(PropertyReader.getrAW_FILE_DIR()).listFiles()) 
			    if (file.getName().equals(fileName)) 
			        file.delete();
				
				_collector.emit(new Values(data));
			}
			
			/**
			 * TESTEND
			 */
			
//			dataList = fileIO.getFiles(PropertyReader.getcLEAN_FILE_DIR());
//			if(dataList.size() > 0){
//				//System.out.println(dataList.size());
//				for( ArrayList<String> dList : dataList){
//					allValues = "";
//					for(String value : dList){
//						allValues += value+",";
//					}
//					//remove trailing colon
//					allValues = allValues.substring(0, allValues.length() -1);
//					
//					_collector.emit(new Values(allValues));
//					
//				}
//			}
		
			
			
			//_collector.emit(new Values(value));
		}

		

		
		
		public void ack(Object msgId) {

		}

		public void fail(Object msgId) {

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("value"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			if (!_isDistributed) {
				Map<String, Object> ret = new HashMap<String, Object>();
//				ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
				return ret;
			} else {
				return null;
			}
		}
	}

	public static class AdderBolt extends BaseRichBolt {
		
		OutputCollector _collector;
		FileWriter fw;
		FileWriter fw2;

		private Double sum = 0d;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			try {
				fw = new FileWriter(new File("/tmp/storm.log"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void execute(Tuple tuple) {
			String temperature = "";
			// check UUID, split tuple and retrieve  temperature value (position 4)
			String data = tuple.getString(0);
			String[] allValues = data.split(",");
			
			ArrayList<String> datalist = new ArrayList<>();
			for(int a = 0; a < allValues.length; a++){
				datalist.add(allValues[a]);
			}
			
			//datalist[0] = filename, has to be used for domBuilder
			//String filename = datalist.remove(0);
			temperature = (String)datalist.get(4);
			//check whether value is within specified range
			String rangeCheckResult = valueChecker.checkRange(temperature);
			//add check result to original xml
		
			String timestamp = String.valueOf(new GregorianCalendar().getTimeInMillis());
			String annotatedXML = domBuilder.buildInsertresultDOM(cleanString, timestamp+".xml", rangeCheckResult);
			
			//FileIO.writeDataFile(annotatedXML, "xml", true, getFileName());
			
			FileIO.writeDataFile(annotatedXML, "xml", true, timestamp);
			
			
				_collector.ack(tuple);
				//cluster.shutdown();
			}

			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				//declarer.declare(new Fields("value"));
				declarer.declare(new Fields("tupel", "date"));
			}

	}


	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
			AuthorizationException, IllegalArgumentException, IOException {
		TopologyBuilder builder = new TopologyBuilder();

		PropertyReader.loadProperties("./conf/qcConf.xml");
		//MultiThreadedSocketServer.startSocket();
		
		builder.setSpout("number", new NumberSpout(), 1);
		builder.setBolt("adder", new AdderBolt(), 3).shuffleGrouping("number");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("BasicExample", conf, builder.createTopology());
			Utils.sleep(10000);
		}
	}


	/**
	 * @return the fileName
	 */
	static String getFileName() {
		return fileName;
	}


	/**
	 * @param fileName the fileName to set
	 */
	static void setFileName(String fileName) {
		BasicExample.fileName = fileName;
	}
	
	
	
	
	
}
