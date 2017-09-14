package de.pangaea.lightning;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketClient {
	private Socket socket;
	
	public SocketClient(String host, int port){
		try {
			socket = new Socket(host, port);
			//sendFile(file);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public void sendTuple(String tuple) throws IOException {
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		
		InputStream inStream = new ByteArrayInputStream(tuple.getBytes(StandardCharsets.UTF_8.name()));
		
		
		//dos.writeUTF(file);
		//dos.writeLong(fileToSend.length());
		byte[] buffer = new byte[(int) tuple.length()];
		//System.out.println(buffer.length);
		int count;// = fis.read(buffer);
		//System.out.println(count);
		while ((count = inStream.read(buffer)) != -1) {
			dos.write(buffer);
		}
		
		inStream.close();
		dos.close();	
	}
}
