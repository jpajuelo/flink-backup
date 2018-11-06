package com.fiupm;

import org.apache.flink.api.java.tuple.Tuple6;

@SuppressWarnings("serial")
public class SpeedRadarEvent extends Tuple6<Long, Integer, Integer, Integer, Integer, Integer>{

	public SpeedRadarEvent(String[] event) {
		super(
				Long.parseLong(event[0]), // time
				Integer.parseInt(event[1]), // vid
				Integer.parseInt(event[3]), // xway
				Integer.parseInt(event[6]), // seg
				Integer.parseInt(event[5]), // dir
				Integer.parseInt(event[2])  // spd
			);
	}
	
	public int getSpeed() {
		return this.f5;
	}
}
