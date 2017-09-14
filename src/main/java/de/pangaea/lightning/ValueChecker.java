package de.pangaea.lightning;

import java.util.ArrayList;

import de.pangaea.lightning.inputprocessing.PropertyReader;

public class ValueChecker {
	
	
	
	protected String checkRange(String value){
		if(Double.valueOf(value) > PropertyReader.gettEMP_THRSHLD_HIGH() || Double.valueOf(value) < PropertyReader.gettEMP_THRSHLD_LOW()){
			return(PropertyReader.getfLAG_RANGE_FAIL());
		}else{
			return(PropertyReader.getfLAG_RANGE_PASS());
		}
	}
	
	protected String checkOutlier(String value){
		ArrayList<String> valueList = new ArrayList(); 
		
		return "";
	}
}

