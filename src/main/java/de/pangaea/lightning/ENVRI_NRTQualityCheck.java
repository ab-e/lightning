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

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

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

public class ENVRI_NRTQualityCheck {

	private static final String envriConsumer01 = "99e7201ccbb29630266fb9fb3911c20055ac5d6e";
	private static final String envriPublisher01 = "f50b7c315eefb3fe7fde05c00751be0115e685a6";
	private static final String SUBSCRIPTIONURL="https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/";
	private static final String TOPICURL="https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/topics/";
	
	public static int  OutlierWindowSize=50;

	private static String fileName = "";

	private static ObjectMapper mapper = new ObjectMapper();
	private static JsonNodeFactory factory = JsonNodeFactory.instance;
	private static HttpClient client = HttpClientBuilder.create().build();	

	public static class MessageReader extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private String subscription ="";
		boolean _isDistributed;
		static SpoutOutputCollector _collector;

		public MessageReader() {
			this(true);
		}
		
		public MessageReader(String subscription) {
			this.subscription=subscription;
		}

		public MessageReader(boolean isDistributed) {
			_isDistributed = isDistributed;
		}

		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
		}

		public void close() {

		}

		public void ackMessage() {
			
		}
		
		public void nextTuple() {
			System.out.println("PULLING MESSAGES...");
			//JsonNode message= new JsonNode();
			try {
				HttpPost request = new HttpPost(
						SUBSCRIPTIONURL+this.getSubscription()+":pull?key="+ envriConsumer01);
				StringEntity postJsonData = new StringEntity("{\"maxMessages\": \"1\"}");
				request.setHeader("Content-type", "application/json");
				request.setEntity(postJsonData);
								
				HttpResponse response = client.execute(request);
				if (response != null) {					
					InputStream in = response.getEntity().getContent();
					//System.out.println(response.getStatusLine());
					JsonNode node = mapper.readTree(in);			
					Iterator<JsonNode> messages = node.get("receivedMessages").elements();					
					while (messages.hasNext()) {
						JsonNode message = messages.next();														
						String ackId = message.get("ackId").asText();	
						_collector.emit(new Values(message));	
						HttpPost ackrequest = new HttpPost(
								SUBSCRIPTIONURL+this.getSubscription()+":acknowledge?key="+ envriConsumer01);
						StringEntity ackJsonData = new StringEntity("{\"ackIds\":[\""+ackId+"\"]}");
						ackrequest.setHeader("Content-type", "application/json");
						ackrequest.setEntity(ackJsonData);
						HttpResponse ackresponse = client.execute(ackrequest);	
						// For some reasons the bolt has to do "something" otherwise the Spout hangs there
						HttpEntity entity = ackresponse.getEntity();						
			            String content = EntityUtils.toString(entity);
			           // System.out.println(content);									
					}	
						
				}
			} catch (Exception ex) {			
				// handle exception here
				System.out.println("ERROR: Could not execute nextTuple: "+ex.getMessage()+" ");
				ex.printStackTrace();
			}	
		}

		public void ack(Object msgId) {

		}

		public void fail(Object msgId) {

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("observationmessage"));
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

		public String getSubscription() {
			return subscription;
		}
	}
	
	
	public static class MessageAtomizer extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {			
			//System.out.println("Atomizer..");			
			JsonNode message= (JsonNode) tuple.getValueByField("observationmessage");			
			JsonNode messageAttributes = message.get("message").get("attributes");						
			String messageType = messageAttributes.get("type").asText();				
			String madeBySensor=messageAttributes.get("madeBySensor").asText();
			String observedProperty=messageAttributes.get("observedProperty").asText();
			String featureOfInterest=messageAttributes.get("hasFeatureOfInterest").asText();
				
			System.out.println("madeBySensor: "+messageAttributes.get("madeBySensor").asText());
			String unit="";
			float[] allowedRange= {0,0};
			SensorMetadata sensor = new SensorMetadata(madeBySensor);
			for(int s= 0; s<sensor.observedProperties.size();s++) {
				if(sensor.observedProperties.get(s).id.equals(observedProperty)) {
					unit=sensor.observedProperties.get(s).unit;
					allowedRange=sensor.observedProperties.get(s).getMeasurementRange();
				}						
			}	
			String messageData = message.get("message").get("data").asText();
			String messageDataDecoded = new String(Base64.getDecoder().decode(messageData));
			if (messageType.equals("AtomicObservation")) {					
				//atomize and emit..	
				_collector.emit(new Values(messageDataDecoded));	
				_collector.ack(tuple);
				
			}else if (messageType.equals("InsertResult")){
				System.out.println("Message Type: InsertResult");
				InsertResultMessage insertMessage = null;
				try {
					insertMessage = new InsertResultMessage(messageDataDecoded);						
					for (Map.Entry<String, String> entry : insertMessage.resultValues.entrySet())
					{
						float resultValue=Float.parseFloat(entry.getValue());
						String resultTime =entry.getKey();
						Observation observation = new Observation(madeBySensor, observedProperty, resultValue);
						observation.setResultTime(resultTime);
						observation.setResultUnit(unit);
						observation.setFeatureOfInterest(featureOfInterest);
						_collector.emit(new Values(observation,allowedRange,observedProperty,madeBySensor));
					}	
					_collector.ack(tuple);
				
				} catch (ParserConfigurationException | SAXException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}				
		}						
	}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("observation","allowedrange","observedProperty","madeBySensor"));
		}

	}
	
	
	public static class OutlierController extends BaseWindowedBolt {
		 //Robust Z-score: https://www.ibm.com/support/knowledgecenter/SSWLVY_1.0.0/com.ibm.spss.analyticcatalyst.help/analytic_catalyst/modified_z.html
		//Value is calculated for the middle value
		   private OutputCollector collector;
		   @Override
		   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		       this.collector = collector;
		   }
		   @Override
		   public void execute(TupleWindow inputWindow) {
			 int windowSize=OutlierWindowSize;
			 double MAD =0;
			 DescriptiveStatistics mad = new DescriptiveStatistics();
			 List<Tuple> tuplesInWindow = inputWindow.get();
		     int windowMid=(int) windowSize/2;
			 double[] valuesToCheck = new double[windowSize];
			 double Zscore =0;
			 String observedPoperty = null;
			 String madeBySensor=null;
			 Observation middleobs = new Observation();
			 int i = 0;
			 if(tuplesInWindow.size()==windowSize) {
			     for(Tuple tuple: tuplesInWindow) {
			    	 Observation obs = (Observation)tuple.getValue(0);			    		 
			    	 observedPoperty=(String) tuple.getValue(1);
			    	 madeBySensor=(String) tuple.getValue(2);
			    	 valuesToCheck[i]= (double) obs.getResultValue();				    	 
			    	 if(i==windowMid) {
			    		 middleobs= obs;
			    	}
			    	i++;
			     }
			     double[] absDeviationFromMedian=new double[valuesToCheck.length];
			     Median m= new Median();
			     double medianValue = m.evaluate(valuesToCheck);
			     //calculating MAD
			     for(int j=0;j<valuesToCheck.length;j++) {
			    	 absDeviationFromMedian[j]=Math.abs(valuesToCheck[j] -medianValue);	
			    	 mad.addValue(absDeviationFromMedian[j]);
			     }
			     MAD =m.evaluate(absDeviationFromMedian);
			     double meanAD = mad.getMean();
			     //calculating modified Z-score M
			    	 if(MAD==0)
			    		 Zscore =Math.abs(valuesToCheck[windowMid]- medianValue)/1.253314*meanAD;
			    	 else
			    		 Zscore =Math.abs(valuesToCheck[windowMid]- medianValue)/1.486*MAD;
			    	 //Zscore[j] =(0.6745*(valuesToCheck[j]- medianValue))/MAD;
			    	 if(Zscore>3.5) {
			    		 //System.out.println("Failed: "+valuesToCheck[middle]+" Z-Score: "+Zscore);							
						middleobs.setQualityOfObservation(1);
			    	}
			    	 else {
			    		 //System.out.println("Passed: "+valuesToCheck[middle]+" Z-Score: "+Zscore); 
			    		 if(middleobs.getQualityOfObservation()!=1)
			    			 middleobs.setQualityOfObservation(0);
			    	}	
			    	collector.emit(new Values(middleobs,observedPoperty,madeBySensor));
			 }else {
				 if(tuplesInWindow.size()<windowMid) {
					 Tuple lasttuple=tuplesInWindow.get(tuplesInWindow.size()-1);
					 Observation lastobs = (Observation)lasttuple.getValue(0);			    		 
			    	 observedPoperty=(String) lasttuple.getValue(1);
			    	 madeBySensor=(String) lasttuple.getValue(2);
			    	 collector.emit(new Values(lastobs,observedPoperty,madeBySensor));
					 //System.out.println(" obs: "+(tuplesInWindow.size()-1)+" "+((Observation) tuplesInWindow.get(tuplesInWindow.size()-1).getValue(0)).getResultValue());
				 }
			 }
			    	
		   }
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("zScoreCheckedObservation", "observedProperty","madeBySensor"));
			}		   
		   
		}
	
	public static class RangeCheckController extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			Observation obs = (Observation)tuple.getValue(0);
			float[] range = (float[]) tuple.getValue(1);
			float value=obs.getResultValue();
			if(range[0]<range[1]) {		
				if(value>=range[0]&&value<=range[1]) {
					//System.out.println("Passed: "+value+" ("+range[0]+" - "+range[1]+")");
					if(obs.getQualityOfObservation()!=1)
						obs.setQualityOfObservation(0);
				}else {
					//System.out.println("Failed: "+value);					
						obs.setQualityOfObservation(1);
				}
			}	
			_collector.emit(new Values(obs,tuple.getValue(2),tuple.getValue(3)));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("rangeCheckedObservation", "observedProperty","madeBySensor"));
		}

	}
	
	public static class QualityControlledMessagePacker extends BaseWindowedBolt {
		   private OutputCollector collector;
		   @Override
		   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		       this.collector = collector;
		   }
		   @Override
		   public void execute(TupleWindow inputWindow) {
			   String message="";
			   ArrayList<Observation> packObs=new ArrayList();
			   List<Tuple> tuplesInWindow = inputWindow.get();
			   String observedProperty="";
			   String madeBySensor="";
			   for(Tuple tuple: tuplesInWindow) {
				   Observation obs = (Observation)tuple.getValue(0);
				   packObs.add(obs);
				   observedProperty=(String) tuple.getValue(1);
				   madeBySensor=(String) tuple.getValue(2);
				   //message+=" "+obs.getResultTime()+" "+obs.getResultValue()+" "+obs.getQualityOfObservation()+"\r\n";
			   }  
			   ObjectMapper mapper = new ObjectMapper();
			  //creating the message we'll send to EGI Argo
			   String datamessage="";
			    String messagehead="{\"messages\": [{\"attributes\":{\"madeBySensor\":\""+madeBySensor+"\",\"observedProperty\":\""+observedProperty+"\"},\"data\":\"";
				try {
					String jsonObs = mapper.writeValueAsString(packObs);
					byte[] encodedjsonObs = Base64.getEncoder().encode(jsonObs.getBytes());
					datamessage=new String(encodedjsonObs);
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				message=messagehead+datamessage+"\"}]}";
				System.out.println("Sending Quality Controlled Message Package");
				//System.out.println(TOPICURL+"envri_topic_101_qc:publish?key="+envriPublisher01);
				HttpPost request = new HttpPost(
						TOPICURL+"envri_topic_101_qc:publish?key="+envriPublisher01) ;
				StringEntity postJsonData = null;
				try {
					postJsonData = new StringEntity(message);
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				request.setHeader("Content-type", "application/json");
				request.setEntity(postJsonData);	
								
				try {
					HttpResponse response = client.execute(request);

				if (response != null) {	
					HttpEntity entity = response.getEntity();	
					System.out.println(response.getStatusLine());
		            String content = EntityUtils.toString(entity);
				}	
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//
			   
		}
	}
	
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("MessageReader", new MessageReader("envri_sub_101"), 1);
		builder.setBolt("MessageAtomizer", new MessageAtomizer(), 1).fieldsGrouping("MessageReader",new Fields("observationmessage"));		
		builder.setBolt("RangeCheckController", new RangeCheckController(), 10).fieldsGrouping("MessageAtomizer",new Fields("observedProperty"));		
		//will fail if more than 20 parameters are submitted in parallel because of grouping -> number of working nodes=20
		builder.setBolt("OutlierController", new OutlierController().withWindow(new Count(OutlierWindowSize), new Count(1)), 20)
	       .fieldsGrouping("RangeCheckController",new Fields("observedProperty"));
		builder.setBolt("QualityControlledMessagePacker", new QualityControlledMessagePacker()
			.withTumblingWindow(new Count(20)), 20)
			.fieldsGrouping("OutlierController",new Fields("observedProperty"));		
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 10000);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(10);
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ENVRI_NRTQualityCheck", conf, builder.createTopology());
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
		ENVRI_NRTQualityCheck.fileName = fileName;
	}

}
