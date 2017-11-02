package de.pangaea.lightning;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import de.pangaea.lightning.MinMaxThresholdQualityCheck.MessageReader;
import de.pangaea.lightning.inputprocessing.FileIO;



public class MultiThreadedSocketServer extends Thread{
	
	
		private ServerSocket socket;
		
			public MultiThreadedSocketServer(int port) {
				try {
					socket = new ServerSocket(port);
				} catch (IOException e) {
					e.printStackTrace();
				}
				Calendar now = Calendar.getInstance();
		        SimpleDateFormat formatter = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
		        System.out.println("Starting socket: "+formatter.format(now.getTime()));
			}
			
			public void run() {
				while (true) {
					try {
						Socket clientSock = socket.accept();
						System.out.println("Accepted Client Address - " + clientSock.getInetAddress().getHostName() + " at " + new GregorianCalendar().getTime());
						//System.out.println("Incoming message: ");
						saveFile(clientSock);
						//emitTuple(clientSock);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			
			
			private void saveFile(Socket clientSock) throws IOException {
				
				DataInputStream dis = new DataInputStream(clientSock.getInputStream());
				

				String filename = new String(new GregorianCalendar().getTime()+".txt");
				FileOutputStream fos = new FileOutputStream(filename);
				
				byte[] buffer = new byte[131072];
				//System.out.println(buffer.length);
				
				int read = 0;
				int totalRead = 0;
				//int remaining = filesize;
				

				
				int count;

				int byteCount = 0;
				String temp  = "";
				
				//dis.readFully(buffer);
				//fos.write(buffer);
				String sAll = "";
				String sTemp;
				while ((count = dis.read(buffer)) !=-1){

						  
					fos.write(buffer,0,count);
					
					
				}
				
				
				fos.close();
				dis.close();
			}
			
			
			
			public static void startSocket(){
				MultiThreadedSocketServer fs = new MultiThreadedSocketServer(9655);
				fs.start();
			}
			
		}

