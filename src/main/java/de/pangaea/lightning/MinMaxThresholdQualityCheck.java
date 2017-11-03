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
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.pangaea.lightning.inputprocessing.DataParser;
import de.pangaea.lightning.inputprocessing.FileIO;
import de.pangaea.lightning.inputprocessing.PropertyReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.org.json.simple.JSONArray;
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

public class MinMaxThresholdQualityCheck {

	private static final String envriConsumer01 = "99e7201ccbb29630266fb9fb3911c20055ac5d6e";
	private static final String envriPublisher01 = "f50b7c315eefb3fe7fde05c00751be0115e685a6";

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

	private static ObjectMapper mapper = new ObjectMapper();
	private static JsonNodeFactory factory = JsonNodeFactory.instance;
	private static HttpClient client = HttpClientBuilder.create().build();

	public static class MessageReader extends BaseRichSpout {

		private FileIO fileIO = null;
		private ArrayList<ArrayList<String>> dataList;
		private String cleanXMLFileName = "";

		boolean _isDistributed;
		static SpoutOutputCollector _collector;

		public MessageReader() {
			this(true);
		}

		public MessageReader(boolean isDistributed) {
			_isDistributed = isDistributed;
		}

		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
			fileIO = new FileIO();
		}

		public void close() {

		}

		public void nextTuple() {
			try {
				HttpPost request = new HttpPost(
						"https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:pull?key="
								+ envriConsumer01);
				StringEntity postJsonData = new StringEntity("{\"maxMessages\": \"1\"}");
				request.setHeader("Content-type", "application/json");
				request.setEntity(postJsonData);
				HttpResponse response = client.execute(request);

				if (response != null) {
					InputStream in = response.getEntity().getContent();
					JsonNode node = mapper.readTree(in);
					Iterator<JsonNode> messages = node.get("receivedMessages").elements();
					
					while (messages.hasNext()) {
						JsonNode message = messages.next();
						String ackId = message.get("ackId").asText();
						JsonNode messageAttributes = message.get("message").get("attributes");
						
						String messageType = messageAttributes.get("type").asText();
						
						if (!messageType.equals("AtomicObservation"))
							continue;
						
						String messageData = message.get("message").get("data").asText();
						String messageDataDecoded = new String(Base64.getDecoder().decode(messageData));
						
						_collector.emit(new Values(messageDataDecoded));
						
						ObjectNode ackIdsJson = factory.objectNode();
						ArrayNode ackIdsArray = factory.arrayNode();
						ackIdsJson.set("ackIds", ackIdsArray);
						ackIdsArray.add(ackId);

						request = new HttpPost(
								"https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:acknowledge?key="
										+ envriConsumer01);
						postJsonData = new StringEntity(mapper.writeValueAsString(ackIdsJson));
						request.setHeader("Content-type", "application/json");
						request.setEntity(postJsonData);
						client.execute(request);
					}
				}
			} catch (Exception ex) {
				// handle exception here
			}

			/**
			 * String templateID = ""; dataStringList.clear(); Utils.sleep(100);
			 * 
			 * Values values = new Values(); String allValues = new String("");
			 * 
			 * 
			 * ArrayList<File> fileList = fileIO.getFiles(); for(int i = 0; i <
			 * fileList.size(); i++){
			 * 
			 * //setFileName(fileList.get(i).getName()); fileName =
			 * fileList.get(i).getName(); cleanString =
			 * fileIO.checkXML(fileList.get(i));
			 * 
			 * try { dataStringList = DataParser.parseInsertResult(cleanString);
			 * templateID = dataStringList.remove(0); } catch
			 * (FileNotFoundException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); } catch (XMLStreamException e) { // TODO
			 * Auto-generated catch block e.printStackTrace(); } String data =
			 * fileIO.splitDataString( dataStringList.get(0) ); data =
			 * templateID.concat(",").concat(data);
			 * 
			 * for(File file: new
			 * File(PropertyReader.getrAW_FILE_DIR()).listFiles()) if
			 * (file.getName().equals(fileName)) file.delete();
			 * 
			 * _collector.emit(new Values(data)); }
			 **/

			/**
			 * TESTEND
			 */

			// dataList = fileIO.getFiles(PropertyReader.getcLEAN_FILE_DIR());
			// if(dataList.size() > 0){
			// //System.out.println(dataList.size());
			// for( ArrayList<String> dList : dataList){
			// allValues = "";
			// for(String value : dList){
			// allValues += value+",";
			// }
			// //remove trailing colon
			// allValues = allValues.substring(0, allValues.length() -1);
			//
			// _collector.emit(new Values(allValues));
			//
			// }
			// }

			// _collector.emit(new Values(value));
		}

		public void ack(Object msgId) {

		}

		public void fail(Object msgId) {

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("observation"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			if (!_isDistributed) {
				Map<String, Object> ret = new HashMap<String, Object>();
				// ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
				return ret;
			} else {
				return null;
			}
		}
	}

	public static class QualityController extends BaseRichBolt {

		OutputCollector _collector;
		FileWriter fw;
		FileWriter fw2;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
//			try {
//				fw = new FileWriter(new File("/tmp/storm.log"));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
		}

		@Override
		public void execute(Tuple tuple) {
			try {
				JsonNode observation = mapper.readTree(tuple.getString(0));
				String resultNumericValue = observation.get("hasResult").get("numericValue").asText();
				
				String rangeCheckResult = valueChecker.checkRange(resultNumericValue);
				
				((ObjectNode)observation).put("qualityOfObservation", rangeCheckResult);
				
				// Translate observation into message and post it to EGI
				String observedProperty = observation.get("observedProperty").asText();
				String sensor = observation.get("madeBySensor").asText();
				String feature = observation.get("hasFeatureOfInterest").asText();
				byte[] bytes = mapper.writeValueAsString(observation).getBytes("UTF-8");
				String messageDataEncoded = Base64.getEncoder().encodeToString(bytes);
				
				ObjectNode messagesJson = factory.objectNode();
				ArrayNode messagesValue = factory.arrayNode();
				messagesJson.set("messages", messagesValue);
				ObjectNode messageJson = factory.objectNode();
				messagesValue.add(messageJson);
				ObjectNode attributesJson = factory.objectNode();
				messageJson.set("attributes", attributesJson);
				attributesJson.put("type", "AnnotatedAtomicObservation");
				attributesJson.put("madeBySensor", sensor);
				attributesJson.put("hasFeatureOfInterest", feature);
				attributesJson.put("observedProperty", observedProperty);
				messageJson.put("data", messageDataEncoded);
	
				HttpPost request = new HttpPost(
						"https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/topics/envri_topic_101:publish?key="
								+ envriPublisher01);
				StringEntity postJsonData = new StringEntity(mapper.writeValueAsString(messagesJson));
				request.setHeader("Content-type", "application/json");
				request.setEntity(postJsonData);
				HttpResponse response = client.execute(request);
				response.getEntity().writeTo(System.out);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			String temperature = "";
//			// check UUID, split tuple and retrieve temperature value (position
//			// 4)
//			String data = tuple.getString(0);
//			String[] allValues = data.split(",");
//
//			ArrayList<String> datalist = new ArrayList<>();
//			for (int a = 0; a < allValues.length; a++) {
//				datalist.add(allValues[a]);
//			}
//
//			// datalist[0] = filename, has to be used for domBuilder
//			// String filename = datalist.remove(0);
//			temperature = (String) datalist.get(4);
//			// check whether value is within specified range
//			String rangeCheckResult = valueChecker.checkRange(temperature);
//			// add check result to original xml
//
//			String timestamp = String.valueOf(new GregorianCalendar().getTimeInMillis());
//			String annotatedXML = domBuilder.buildInsertresultDOM(cleanString, timestamp + ".xml", rangeCheckResult);
//
//			// FileIO.writeDataFile(annotatedXML, "xml", true, getFileName());
//
//			FileIO.writeDataFile(annotatedXML, "xml", true, timestamp);
//
			_collector.ack(tuple);
//			// cluster.shutdown();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// declarer.declare(new Fields("value"));
			// declarer.declare(new Fields("tupel", "date"));
			declarer.declare(new Fields("observation"));
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
			AuthorizationException, IllegalArgumentException, IOException {
		TopologyBuilder builder = new TopologyBuilder();

		PropertyReader.loadProperties("./conf/qcConf.xml");
		// MultiThreadedSocketServer.startSocket();

		builder.setSpout("MessageReader", new MessageReader(), 1);
		builder.setBolt("QualityController", new QualityController(), 3).shuffleGrouping("MessageReader");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MinMaxThresholdQualityCheck", conf, builder.createTopology());
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
	 * @param fileName
	 *            the fileName to set
	 */
	static void setFileName(String fileName) {
		MinMaxThresholdQualityCheck.fileName = fileName;
	}

}
