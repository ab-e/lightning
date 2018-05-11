package de.pangaea.lightning;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;

import javax.xml.xpath.XPathFactory;

import org.apache.storm.shade.org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;



public class SensorMetadata {	
	private String name="";
	private String description;
	private URL url;
	private String id;
	public ArrayList<ObservedProperty> observedProperties = new ArrayList<ObservedProperty>();

	
	static class ObservedProperty{
		public String name;
		public String id;
		public String unit;
		public float[] measurementRange = new float[2];	
		
		public ObservedProperty() {

		}
		
		public ObservedProperty(String name, String id, String unit) {
			this.name=name;
			this.id=id;
			this.unit=unit;
		}

		public float[] getMeasurementRange() {
			return measurementRange;
		}

		public void setMeasurementRange(float minRange, float maxRange) {
			this.measurementRange[0]=minRange;
			this.measurementRange[1]=maxRange;
		}
	}
	
	public String download(URL url) {
		String ret = null;
		try {
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");				
			if(con.getResponseCode()==HttpURLConnection.HTTP_OK){
				if(isValidContentType(con.getContentType())) {
					BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
					String inputLine;
					StringBuffer content = new StringBuffer();
					while ((inputLine = in.readLine()) != null) {
						content.append(inputLine);
					}
					in.close();
					if(con.getContentType().contains("xml")) {
						ret=content.toString();
					}
					else {
						System.out.println("Invalid Http content type: "+con.getContentType());
					}
			}else System.out.println("Invalid response: "+con.getContentType());
			}			
			con.disconnect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
		
	public void setSensorMetadata(String content) {		
			//SensorML
			if(content.contains("http://www.opengis.net/sensorml/2.0")) {
				try {
					this.parseSensorML(content);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			//internal JSON
			else if(content.contains("observedProperties")){
				try {
					this.parseSensorJSON(content);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//other formats here..
			else {
				System.out.println("Unknown Format");
				
			}
		}
	

		
	private void parseSensorJSON(String content) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readValue(content, JsonNode.class);
		//SensorMetadata s = mapper.readValue(content, SensorMetadata.class);
		this.id=node.get("id").asText();
		if(node.get("url") != null) {
			String urlString=node.get("url").asText();
			this.url= new URL(urlString); 
		}
		this.name=node.get("name").asText();
		this.description=node.get("description").asText();
		if(node.get("observedProperties")!=null) {
			JsonNode propNode=node.get("observedProperties");
			if (propNode.isArray()) {
				for (JsonNode pnode : propNode) {
					this.observedProperties.add(mapper.convertValue(pnode,ObservedProperty.class));
				}
			}
		}
	}

	private boolean isValidContentType(String type) {
		String[] validTypes= {"application/xml","text/xml","application/json"};
		if(Arrays.asList(validTypes).contains(type)) {
			return true;
		} else return false;
	}
	
	
	//Option 1: SensorML as source for the Sensor metadata:
	private void parseSensorML(String sml) throws Exception{	

		String[] measurementRangeURIs= {"http://vocab.ndg.nerc.ac.uk/term/w04/current/CAPB0006",
				"http://vocab.nerc.ac.uk/term/w04/current/CAPB0006",
				"https://www.w3.org/TR/vocab-ssn/#SSNSYSTEMMeasurementRange",
				"https://www.w3.org/2005/Incubator/ssn/ssnx/ssn#MeasurementRange"};

	    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    factory.setNamespaceAware(true);
	    DocumentBuilder builder;
		builder = factory.newDocumentBuilder();
	    Document doc = builder.parse(new InputSource(new StringReader(sml)));
	    this.setDescription(doc.getElementsByTagName("gml:description").item(0).getTextContent());
	    this.setName(doc.getElementsByTagName("gml:name").item(0).getTextContent());
	    XPath xpath = XPathFactory.newInstance().newXPath();
	    xpath.setNamespaceContext(new OGCNameSpaceContext());
	    //xpath jumps to swe:Quantity but not the one within swe:quality
	    XPathExpression expr = xpath.compile("//sml:output//swe:Quantity[not(ancestor::swe:quality)]");
	    NodeList nodeList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
	    for (int i = 0; i < nodeList.getLength(); ++i) {
			String paramUnit= "";
			String paramName = "";
			String paramID = "";
			String[] paramRanges = new String[2];
	        Node node = nodeList.item(i);
	        NodeList cnodeList= node.getChildNodes();
	        for (int j=0; j < cnodeList.getLength() ; ++j) {
	        	Node cNode =cnodeList.item(j);
	        	Element e= (Element)cNode;
	        	if(cNode.getNodeName().equals("swe:label")){
	        		paramName = cNode.getTextContent();
	        	}else if(cNode.getNodeName().equals("swe:uom")){
	        		paramUnit = e.getAttribute("code");
	        	}else if(cNode.getNodeName().equals("swe:quality")){
	        		//MeasurementRange	  element has to be QuantityRange and be of type SDN CAPB0006	        		
	        		if(cNode.getFirstChild().getNodeName().equals("swe:QuantityRange") && 
	        				Arrays.asList(measurementRangeURIs).contains(cNode.getFirstChild().getAttributes().item(0).getTextContent())) {
	        			NodeList qNode=cNode.getFirstChild().getChildNodes();
	        			for (int k=0; k < qNode.getLength() ; ++k) {
	        				if(qNode.item(k).getNodeName().equals("swe:value")) {
	        					paramRanges =qNode.item(k).getTextContent().split("\\s");
	        				}
	        			}
	        		}
	        		      		
	        	}
	        }
	        paramID =  node.getAttributes().item(0).getTextContent();
	        observedProperties.add(i,new ObservedProperty(paramName, paramID, paramUnit));
	        if(paramRanges.length==2) {
	        	try {
	        		   if(paramRanges[0]!=null && paramRanges[1]!=null) {
	        			observedProperties.get(i).setMeasurementRange(Float.parseFloat(paramRanges[0]), Float.parseFloat(paramRanges[1]));
	        		}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("SensorML: Invalid Number Format for MeasurementRange");
				}	     
	        }
	      }
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public URL getUrl() {
		return url;
	}

	private boolean setUrl(String uri) {		
	    try {
	        URL url = new URL(uri); 
	        this.url = url;
	    } catch (MalformedURLException e) {
	        return false;
	    }
	   
	    return true;
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public SensorMetadata() {
		
	}
	
	
	public SensorMetadata(String id) {
		this.setId(id);
		if(setUrl(id)) {
		String jsonPath="sensorinfo/"+id.hashCode()+".json";
		//check for cached version
		File f = new File(jsonPath);
		if(f.exists()) {
			String json = null;
			try {
				json = new String(Files.readAllBytes(Paths.get(jsonPath)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.setSensorMetadata(json);
		}
		//new one
		else {
			//if the Uri is actionable we try to retrieve the content
			String content=this.download(this.url);
			if(!content.isEmpty()) {
				this.setSensorMetadata(content);				
				ObjectMapper mapper = new ObjectMapper();
				
				try {
					mapper.writeValue(new File(jsonPath),this);
				} catch (JsonGenerationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.println("sensorinfo/"+FilenameUtils.getName(this.url.getPath()));
				//mapper.writeValue(new File("sensorinfo/"+FilenameUtils.getName(this.url.getPath()).replace(".xml", ".json")), this);
			}			
		}
	}else {
			System.out.println("Invalid URL: "+id);
		}
	}
	
	public static void main(String[] args) {
		String uri = "http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml";
		//String uri = "http://dataportals.pangaea.de/sml/t-sos/pres_template.xml";
		SensorMetadata Sensor = new SensorMetadata(uri);
		System.out.println(Sensor.getName());
		System.out.println(Sensor.getDescription());
		for(int i=0; i<Sensor.observedProperties.size();i++) {
			System.out.println(i);
			System.out.println(Sensor.observedProperties.get(i).id);
			System.out.println(Sensor.observedProperties.get(i).name);
			System.out.println(Sensor.observedProperties.get(i).unit);
			System.out.print(Sensor.observedProperties.get(i).measurementRange[0]+" - ");
			System.out.println(Sensor.observedProperties.get(i).measurementRange[1]);
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}


