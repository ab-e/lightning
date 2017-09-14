package de.pangaea.lightning.inputprocessing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import javax.xml.stream.*;
import javax.xml.transform.Source;



public class DataParser {
	
	private static XMLInputFactory inputFactory;
	private static InputStream input;
	private static XMLStreamReader xmlStreamReader;
	
	public static synchronized ArrayList<String> parseInsertResult(String inString) throws XMLStreamException, FileNotFoundException{
		ArrayList<String> returnList = new ArrayList<>();
		
		inputFactory = XMLInputFactory.newInstance();
		input = new ByteArrayInputStream(inString.getBytes(StandardCharsets.UTF_8));
		
		xmlStreamReader = inputFactory.createXMLStreamReader(input);
		
		while(xmlStreamReader.hasNext()){
			
			int event=xmlStreamReader.next();
			if(event==XMLStreamConstants.START_ELEMENT){
			
				if(xmlStreamReader.getLocalName().equals("template")){
					//System.out.println(xmlStreamReader.getElementText());
					returnList.add(xmlStreamReader.getElementText().trim());
				}
				if(xmlStreamReader.getLocalName().equals("resultValues")){
					//System.out.println(xmlStreamReader.getElementText());
					returnList.add(xmlStreamReader.getElementText().trim());
				}
			}
		}
		return returnList;
	}
}
