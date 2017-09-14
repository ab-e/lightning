package de.pangaea.lightning.inputprocessing;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.GregorianCalendar;

import org.apache.storm.shade.org.apache.commons.io.monitor.FileAlterationListener;

import de.pangaea.lightning.SocketClient;

public class InputProcessor {

	private static String sourceDir = "./raw_datafiles/";
	private static ArrayList<ArrayList<String>> fileList = new ArrayList<>();
	private static FileIO fileIO = new FileIO();
	
	//private static SocketClient socketClient = new SocketClient("localhost", 9655);
	
	public static void main(String[] args) {
		PropertyReader.loadProperties("./conf/qcConf.xml");
		// TODO Auto-generated method stub
		fileList = fileIO.getFiles(PropertyReader.getrAW_FILE_DIR());
		//System.out.println(fileList.get(0).toString());
		
		for(File file: new File(PropertyReader.getrAW_FILE_DIR()).listFiles()) 
		    if (!file.isDirectory()) 
		        file.delete();
		
		//submit tuples to storm topology
		for(int i = 0; i < fileList.size(); i++){
			ArrayList<String> temp = fileList.get(i);
			String sTemp = "";
			for(int z = 0; z < temp.size(); z++){
				sTemp = sTemp.concat(temp.get(z) + ",");
			}
			
//			try {
//				socketClient.sendTuple(sTemp);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
			fileIO.writeDataFile(sTemp, "txt", false, "");
		}
	}
	
	

}
