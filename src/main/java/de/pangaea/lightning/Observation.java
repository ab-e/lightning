package de.pangaea.lightning;

import java.util.ArrayList;

public class Observation {
	private String madeBySensor;
	private String observedProperty;
	private String hasFeatureOfInterest;
	private int qualityOfObservation=0; //0=good; 1=bad
	//public ArrayList<Rating> qualityRating = new ArrayList(); 
	private String id;
	private Result hasResult;
	private String resultTime;
	
	//schema.org style quality annotation
	public class Rating{
		public String reviewAspect;
		public int ratingValue;
	}
	
	public class Result {
		
		private String id;
		
		public Result(float value, String unit) {
			this.numericValue=value;
			this.unit=unit;
		}
		
		public Result(float value) {
			this.numericValue=value;
		}
		
		private float numericValue;
		private String unit;
			
		public float getNumericValue() {
			return numericValue;
		}

		public void setNumericValue(float numericValue) {
			this.numericValue = numericValue;
		}

		public String getUnit() {
			return unit;
		}

		public void setUnit(String unit) {
			this.unit = unit;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}
	
	public Observation(){
		
	}
	
	public Observation(String sensor, String property, float value) {
		this.setMadeBySensor(sensor);
		this.setObservedProperty(property);
		this.setHasResult(new Result(value));
	}

	public String getFeatureOfInterest() {
		return hasFeatureOfInterest;
	}

	public void setFeatureOfInterest(String hasFeatureOfInterest) {
		this.hasFeatureOfInterest = hasFeatureOfInterest;
	}

	public void setResultUnit(String unit) {
		this.getHasResult().setUnit(unit);
	}
	
	public String getResultUnit() {
		return this.getHasResult().getUnit();
	}
	
	public float getResultValue() {
		return this.getHasResult().getNumericValue();
	}

	public String getResultTime() {
		return resultTime;
	}

	public void setResultTime(String resultTime) {
		this.resultTime = resultTime;
	}

	public String getObservedProperty() {
		return observedProperty;
	}

	public void setObservedProperty(String observedProperty) {
		this.observedProperty = observedProperty;
	}

	public String getMadeBySensor() {
		return madeBySensor;
	}

	public void setMadeBySensor(String madeBySensor) {
		this.madeBySensor = madeBySensor;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Result getHasResult() {
		return hasResult;
	}

	public void setHasResult(Result hasResult) {
		this.hasResult = hasResult;
	}

	public int getQualityOfObservation() {
		return qualityOfObservation;
	}

	public void setQualityOfObservation(int qualityOfObservation) {
		this.qualityOfObservation = qualityOfObservation;
	}


}
