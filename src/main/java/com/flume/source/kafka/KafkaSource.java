/*******************************************************************************
 * Copyright 2013 Renjie Yao
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.flume.source.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flume.utils.KafkaUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSource extends AbstractSource implements Configurable, PollableSource {

	/**
	 * @param args
	 */
	private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
	private ConsumerConnector consumer;
	private ConsumerIterator<byte[], byte[]> it;
	private String topic;

	public Status process() throws EventDeliveryException {
		List<Event> eventList = new ArrayList<Event>();
		byte [] bytes;
		try {
			if(it.hasNext()) {
                Event event = new SimpleEvent();
                Map<String, String> headers = new HashMap<String, String>();
				headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
				bytes = it.next().message();
                log.debug("Message: {}", new String(bytes));
				event.setBody(bytes);
				event.setHeaders(headers);
				eventList.add(event);
			}
			getChannelProcessor().processEventBatch(eventList);
			return Status.READY;
		} catch (Exception e) {
			log.error("KafkaSource EXCEPTION, {}", e);
			return Status.BACKOFF;
		}
	}

	public void configure(Context context) {
		this.topic = context.getString("topic");
		try {
			this.consumer = KafkaUtil.getConsumer(context);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
	    it = stream.iterator();
	}

	@Override
	public synchronized void stop() {
		consumer.shutdown();
		super.stop();
	}

}
