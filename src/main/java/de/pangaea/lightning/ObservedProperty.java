package de.pangaea.lightning;

class ObservedProperty{
	private String name;
	private String id;
	private String unit;
	private float[] measurementRange = new float[2];	
	
	public ObservedProperty(String name, String id, String unit) {
		this.name=name;
		this.id=id;
		this.unit=unit;
	}

	public float[] getMeasurementRange() {
		return measurementRange;
	}

	public void setMeasurementRange(float minRange, float maxRange) {
		this.measurementRange[0]=minRange;
		this.measurementRange[1]=maxRange;
	}
}
