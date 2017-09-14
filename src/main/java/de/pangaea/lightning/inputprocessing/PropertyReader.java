package de.pangaea.lightning.inputprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;



public class PropertyReader {

	private static Properties properties = new Properties();
	
	private static double tEMP_THRSHLD_LOW;
	private static double tEMP_THRSHLD_HIGH;
	
	//flags according to CORIOLIS flagging scheme
	private static String fLAG_RANGE_PASS;
	private static String fLAG_RANGE_FAIL;
	
	private static String rAW_FILE_DIR;
	private static String cLEAN_FILE_DIR;
	private static String aNNOTATED_FILE_DIR;
	private static String sTRIPPED_DATA_DIR;
	
	private static String cTD_UUID;
	private static String fLUOROMETER_UUID;
	
	
	public static void loadProperties(String fileName )  {
			
			try {
				properties.loadFromXML(new FileInputStream(new File(fileName)));
			} catch (InvalidPropertiesFormatException e) {
				
			} catch (IOException e) {
				
			}
			
			
			settEMP_THRSHLD_LOW( Double.valueOf((String)properties.get("TEMP_THRSHLD_LOW")) );
			settEMP_THRSHLD_HIGH( Double.valueOf((String)properties.get("TEMP_THRSHLD_HIGH")) );
			
			setfLAG_RANGE_PASS( (String)properties.get("FLAG_RANGE_PASS") );
			setfLAG_RANGE_FAIL( (String)properties.get("FLAG_RANGE_FAIL") );
			
			setrAW_FILE_DIR( (String)properties.get("RAW_FILE_DIR") );
			setcLEAN_FILE_DIR( (String)properties.get("CLEAN_FILE_DIR") );
			setaNNOTATED_FILE_DIR( (String)properties.get("ANNOTATED_FILE_DIR") );			
			setsTRIPPED_DATA_DIR((String)properties.get("STRIPPED_DATA_DIR"));
			
			
		}


	/**
	 * @return the tEMP_THRSHLD_LOW
	 */
	public static double gettEMP_THRSHLD_LOW() {
		return tEMP_THRSHLD_LOW;
	}


	/**
	 * @param tEMP_THRSHLD_LOW the tEMP_THRSHLD_LOW to set
	 */
	static void settEMP_THRSHLD_LOW(double tEMP_THRSHLD_LOW) {
		PropertyReader.tEMP_THRSHLD_LOW = tEMP_THRSHLD_LOW;
	}


	/**
	 * @return the tEMP_THRSHLD_HIGH
	 */
	public static double gettEMP_THRSHLD_HIGH() {
		return tEMP_THRSHLD_HIGH;
	}


	/**
	 * @param tEMP_THRSHLD_HIGH the tEMP_THRSHLD_HIGH to set
	 */
	static void settEMP_THRSHLD_HIGH(double tEMP_THRSHLD_HIGH) {
		PropertyReader.tEMP_THRSHLD_HIGH = tEMP_THRSHLD_HIGH;
	}


	/**
	 * @return the fLAG_RANGE_PASS
	 */
	public static String getfLAG_RANGE_PASS() {
		return fLAG_RANGE_PASS;
	}


	/**
	 * @param fLAG_RANGE_PASS the fLAG_RANGE_PASS to set
	 */
	static void setfLAG_RANGE_PASS(String fLAG_RANGE_PASS) {
		PropertyReader.fLAG_RANGE_PASS = fLAG_RANGE_PASS;
	}


	/**
	 * @return the rAW_FILE_DIR
	 */
	public static String getrAW_FILE_DIR() {
		return rAW_FILE_DIR;
	}


	/**
	 * @param rAW_FILE_DIR the rAW_FILE_DIR to set
	 */
	static void setrAW_FILE_DIR(String rAW_FILE_DIR) {
		PropertyReader.rAW_FILE_DIR = rAW_FILE_DIR;
	}


	/**
	 * @return the cLEAN_FILE_DIR
	 */
	public static String getcLEAN_FILE_DIR() {
		return cLEAN_FILE_DIR;
	}


	/**
	 * @param cLEAN_FILE_DIR the cLEAN_FILE_DIR to set
	 */
	static void setcLEAN_FILE_DIR(String cLEAN_FILE_DIR) {
		PropertyReader.cLEAN_FILE_DIR = cLEAN_FILE_DIR;
	}


	/**
	 * @return the aNNOTATED_FILE_DIR
	 */
	public static String getaNNOTATED_FILE_DIR() {
		return aNNOTATED_FILE_DIR;
	}


	/**
	 * @param aNNOTATED_FILE_DIR the aNNOTATED_FILE_DIR to set
	 */
	static void setaNNOTATED_FILE_DIR(String aNNOTATED_FILE_DIR) {
		PropertyReader.aNNOTATED_FILE_DIR = aNNOTATED_FILE_DIR;
	}


	/**
	 * @return the sTRIPPED_DATA_DIR
	 */
	static String getsTRIPPED_DATA_DIR() {
		return sTRIPPED_DATA_DIR;
	}


	/**
	 * @param sTRIPPED_DATA_DIR the sTRIPPED_DATA_DIR to set
	 */
	static void setsTRIPPED_DATA_DIR(String sTRIPPED_DATA_DIR) {
		PropertyReader.sTRIPPED_DATA_DIR = sTRIPPED_DATA_DIR;
	}


	/**
	 * @return the fLAG_RANGE_FAIL
	 */
	public static String getfLAG_RANGE_FAIL() {
		return fLAG_RANGE_FAIL;
	}


	/**
	 * @param fLAG_RANGE_FAIL the fLAG_RANGE_FAIL to set
	 */
	static void setfLAG_RANGE_FAIL(String fLAG_RANGE_FAIL) {
		PropertyReader.fLAG_RANGE_FAIL = fLAG_RANGE_FAIL;
	}


	/**
	 * @return the cTD_UUID
	 */
	static String getcTD_UUID() {
		return cTD_UUID;
	}


	/**
	 * @param cTD_UUID the cTD_UUID to set
	 */
	static void setcTD_UUID(String cTD_UUID) {
		PropertyReader.cTD_UUID = cTD_UUID;
	}


	/**
	 * @return the fLUOROMETER_UUID
	 */
	static String getfLUOROMETER_UUID() {
		return fLUOROMETER_UUID;
	}


	/**
	 * @param fLUOROMETER_UUID the fLUOROMETER_UUID to set
	 */
	static void setfLUOROMETER_UUID(String fLUOROMETER_UUID) {
		PropertyReader.fLUOROMETER_UUID = fLUOROMETER_UUID;
	}
	
}
