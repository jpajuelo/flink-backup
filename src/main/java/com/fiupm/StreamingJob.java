package com.fiupm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flinkproject-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c com.fiupm.StreamingJob target/flinkproject-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		
		String inFilePath = args[0];
		String outFilePath = args[1];
		
		System.out.println("Starting...");
		
		DataStreamSource<String> source = env.readTextFile(inFilePath);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// time, vid, xway, seg, dir, spd

		SingleOutputStreamOperator<SpeedRadarEvent> stream1 = source
			.map(new MapFunction<String, SpeedRadarEvent>() {

				@Override
				public SpeedRadarEvent map(String input) throws Exception {
					// TODO Auto-generated method stub
					return new SpeedRadarEvent(input.split(","));
				}
				
			})
			.filter(new FilterFunction<SpeedRadarEvent>() {

				@Override
				public boolean filter(SpeedRadarEvent event) throws Exception {
					// TODO Auto-generated method stub
					return event.getSpeed() > 90;
				}
			});
		
		System.out.println("Exporting...");
		
		stream1.writeAsCsv(outFilePath + "/speedfines.csv");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
