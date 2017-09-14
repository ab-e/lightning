package de.pangaea.lightning;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.pangaea.lightning.inputprocessing.PropertyReader;

public class DOMBuilder {
	private static Document doc = null;
	private static InputStream inStream = null;
	
	
	public synchronized String buildInsertresultDOM(String in, String filename, String rangeCheckResult){
		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		javax.xml.transform.stream.StreamResult result = null;
		
		try {
			InputSource inSource = new InputSource(inStream = new ByteArrayInputStream(in.getBytes("UTF-8")));
			
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			doc = docBuilder.parse(inSource);
			Node node = doc.getFirstChild();
			NodeList nList = node.getChildNodes();
			Node valueNode = null;
			String checkedResult = "";
			for(int i = 0; i < nList.getLength(); i++){
				if(nList.item(i).getNodeName().equals("sos:resultValues")){
					//System.out.println(nList.item(i).getTextContent());
					valueNode = nList.item(i);
					ArrayList<String> valueList = new ArrayList<String>(Arrays.asList(valueNode.getTextContent().split("#")));
					

					/**
					 * perform checks and add results to original value string
					 */
					String temperature = valueList.get(3);
					//System.out.println(temperature);
					valueList.set(4, rangeCheckResult);
					//valueList.set(5, "fail");
					checkedResult = new String();
					for(int v = 0; v < valueList.size(); v++){
						checkedResult = checkedResult.concat(valueList.get(v)+"#");
					}
					//nList.item(i).setTextContent()
				}
			}
			
			//System.out.println(node.getTextContent());
			valueNode.setTextContent(checkedResult);
			
			//System.out.println(node.getNodeName());
			
			javax.xml.transform.TransformerFactory transformerFactory = javax.xml.transform.TransformerFactory.newInstance();
			javax.xml.transform.Transformer transformer = null;
			try {
				transformer = transformerFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				
				System.out.println(e.getMessage());
			}
			javax.xml.transform.dom.DOMSource source = new javax.xml.transform.dom.DOMSource(doc);
			
			OutputStream out = new ByteArrayOutputStream();
			result = new javax.xml.transform.stream.StreamResult(new File(PropertyReader.getaNNOTATED_FILE_DIR() + "/" + filename + ".xml"));
			result.setOutputStream(out);
			
			try {
				transformer.transform(source, result);
			} catch (javax.xml.transform.TransformerException e) {
				System.out.println(e.getMessage());
				
			}
			//System.out.println(result.getOutputStream().toString());
			
			
			

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(result.getOutputStream().toString());
		return  (result.getOutputStream().toString() );
	}

	
	
}
