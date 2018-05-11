package de.pangaea.lightning;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class InsertResultMessage extends ENVRI_NRTQualityCheck {
	public String content="";
	public String templateID="";
	public URL templateURL;
	public String insertResultTemplate="";
	public String tokenSeparator="#";
	public String blockSeparator="@@";
	public String resultValuesString="";
	
	public Map<String, String> resultValues = new LinkedHashMap<String, String>();	
	
	
	public void splitResultValues(){
		if(!this.resultValuesString.isEmpty()) {
			String[] rows = this.resultValuesString.split(this.blockSeparator);
			for(int i=0;i<rows.length;i++) {
				String[] tokens = rows[i].split(this.tokenSeparator);	
				this.resultValues.put(tokens[0],tokens[1]);
			}
		}
	}
	
	public void initializeTemplateValues(URL url) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(url.openStream());
		//String sensorID=doc.getElementsByTagName("om:procedure").item(0).getAttributes().item(0).getTextContent();

	}
	
	
	public InsertResultMessage(String message) throws ParserConfigurationException, SAXException, IOException{
		this.content=message;
		//System.out.println(message);
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(this.content)));
		try {
			this.templateID=doc.getElementsByTagName("sos:template").item(0).getTextContent();			
			this.templateURL= new URL(templateID);	
			initializeTemplateValues(templateURL);		
			this.resultValuesString=doc.getElementsByTagName("sos:resultValues").item(0).getTextContent();	
			this.splitResultValues();
			
		}catch  (Exception e){
			System.out.println("Error during Template loading.."+e.getMessage());
		}
	   
	}
	
}
