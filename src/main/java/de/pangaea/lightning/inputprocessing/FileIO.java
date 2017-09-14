package de.pangaea.lightning.inputprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.GregorianCalendar;

import javax.xml.stream.XMLStreamException;

import de.pangaea.lightning.DOMBuilder;

public class FileIO {
	
	//private String sourceDir = "./datafiles/";
	private static String sourceDir = "/home/abe/workspace/lightning/raw_datafiles/";
	private static String destDir = "./invalidXML/";
	
	private static ArrayList<ArrayList<String>> resultList = new ArrayList<>();
	private static ArrayList<File> fileList = new ArrayList<>();
	private static File[] xmlFiles;
	
	private static DOMBuilder domBuilder = new DOMBuilder();
	private static String fileName = "";
	
	
	
	public ArrayList<File> getFiles(){
		fileList.clear();
		xmlFiles = new File(PropertyReader.getrAW_FILE_DIR()).listFiles();
		for (File xmlFile : xmlFiles){
			fileList.add(xmlFile);
		}
		return fileList;
	}
	
	
	
	
	
	
	
	
	
	public ArrayList<ArrayList<String>> getFiles(String directory){
		resultList.clear();
		
		xmlFiles = new File(directory).listFiles();
		for (File xmlFile : xmlFiles){
			
			if(xmlFile.isFile()){
				fileName = xmlFile.getName().substring(0, xmlFile.getName().length() - 4);
				ArrayList<String> inputList = new ArrayList<>();
				inputList = extractData(xmlFile);
				if(inputList != null){
					resultList.add( prepareData(inputList) );
				}
			}
		}
		//this.setResultList(resultList);
		
//		for(File file: new File(directory).listFiles()) 
//	    if (!file.isDirectory()) 
//	        file.delete();
		
		//write datastrings to file 
//		for(int i = 0; i < resultList.size(); i++){
//			ArrayList<String> temp = resultList.get(i);
//			String sTemp = "";
//			for(int z = 0; z < temp.size(); z++){
//				sTemp = sTemp.concat(temp.get(z) + ",");
//			}
//			//writeDataFile(sTemp, "txt");
//		}
		
		return resultList;
	}
	
	/**
	 * @return the resultList
	 */
	public ArrayList<ArrayList<String>> getResultList() {
		return resultList;
	}

	/**
	 * @param resultList the resultList to set
	 */
	public void setResultList(ArrayList<ArrayList<String>> resultList) {
		this.resultList = resultList;
	}

	
	public synchronized String checkXML(File xmlFile){
		
		StringBuffer checkBuffer = readFile(xmlFile.getAbsolutePath());
		String checkString = checkBuffer.toString();
		int start = checkString.indexOf("<?xml");
		int end = checkString.lastIndexOf("</sos:InsertResult>");
		String cleanString = checkString.substring(start, end+19);
		
		return cleanString;
		
	}
	
	private static ArrayList<String> extractData(File xmlFile){
		ArrayList<String> dataList = new ArrayList<>();
		dataList.clear();
		
		/**
		 * check if xml is valid - remove preceding and trailing characters
		 * 
		 */
		StringBuffer checkBuffer = readFile(xmlFile.getAbsolutePath());
		String checkString = checkBuffer.toString();
		int start = checkString.indexOf("<?xml");
		int end = checkString.lastIndexOf("</sos:InsertResult>");
		String cleanString = checkString.substring(start, end+19);
		//System.out.println(cleanString);
		
		//write valid xml to file
		writeDataFile(cleanString, "xml", false, fileName);
		
		
		//String resxml = domBuilder.buildInsertresultDOM(cleanString);

		
		
		
//		if(cleanString.contains("291EC6DA-43F8-46D3-86F5-628FBF955261")){
//			try {
//				Files.copy(Paths.get(sourceDir+xmlFile.getName()), Paths.get("/home/abe/temp/stormtest/datafiles/291EC6DA-43F8-46D3-86F5-628FBF955261/"+xmlFile.getName()), StandardCopyOption.REPLACE_EXISTING );
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}else{
//			try {
//				Files.copy(Paths.get(sourceDir+xmlFile.getName()), Paths.get("/home/abe/temp/stormtest/datafiles/83364A4B-7569-41EC-8622-58ADC7760CF7/"+xmlFile.getName()), StandardCopyOption.REPLACE_EXISTING );
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		
		try {
			//dataList = DataParser.parseInsertResult(xmlFile);
			dataList = DataParser.parseInsertResult(cleanString);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			
			//move invalid xml to specified directory
			//System.out.println("Parsing error - moving invalid XML.");
			try {
				Files.copy(Paths.get(sourceDir+xmlFile.getName()), Paths.get(destDir+xmlFile.getName()), StandardCopyOption.REPLACE_EXISTING );
				dataList = null;
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		return dataList;
	}
	
	public synchronized String splitDataString(String data){

		data = data.replace("#", "," );
		if(data.endsWith("@")){
			data = data.substring(0, data.length() -1 );
		}
		
		return data;
	}
	
	
	private static ArrayList<String> prepareData(ArrayList<String> inList){
		ArrayList<String> dataPointList = new ArrayList<>();
		String templateID = inList.remove(0);
		
		//split into data packets
		String[] tempArr = inList.remove(0).split("@");
		
		for(int i = 0; i < tempArr.length; i++){
			String dataString = tempArr[i];
			String[] dataArr = dataString.split("#");
			dataPointList.add(0, templateID);
			for(int k = 0; k < dataArr.length; k++){
				dataPointList.add(dataArr[k]);
			}
			
		}
		//add filename to first position 
		dataPointList.add(0, fileName);
		return dataPointList;
	}
	
	public static StringBuffer readFile(String fileName) {
		StringBuffer buffer = new StringBuffer();

		File file = new File(fileName);

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		char[] buf = new char[1024];
		int numRead = 0;
		try {
			while ((numRead = reader.read(buf)) != -1) {
				String readData = String.valueOf(buf, 0, numRead);
				buffer.append(readData);

				buf = new char[1024];
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			reader.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return buffer;

	}
	
	/**
	 * method is used to temporarily write datafiles to disk. Should be replaced by http / socket stream
	 * @param data
	 */
	public static synchronized void writeDataFile(String data, String mime, boolean bAnnotated, String fileName) {
		String filename = "";
		
		if(mime.equals("xml") && !bAnnotated){
			filename = new String(PropertyReader.getcLEAN_FILE_DIR() + "/" + fileName + "." + mime);
		}else if(mime.equals("txt")){
			filename = new String(PropertyReader.getsTRIPPED_DATA_DIR() + "/" + fileName + "." + mime);
		}else{
			filename = new String(PropertyReader.getaNNOTATED_FILE_DIR() + "/" + fileName  + "." + mime);
		}
		
		
		File f = new File(filename);
		
		
		try {
			FileWriter fWriter = new FileWriter(f);
			fWriter.write(data);
			fWriter.flush();
			fWriter.close();
		} catch (IOException e) {
			System.out.println("Error writing file: "+e.getMessage());
		}
	}
}