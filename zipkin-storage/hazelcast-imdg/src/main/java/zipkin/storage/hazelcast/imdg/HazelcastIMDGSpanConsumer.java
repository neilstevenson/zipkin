/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.hazelcast.imdg;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;

import java.util.List;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import zipkin.Span;
import zipkin.storage.StorageAdapters.SpanConsumer;

/**
 * <P>Consumer of spans to put into Hazelcast storage, that will
 * most likely be invoked asynchronously in an executor thread.
 * </P>
 */
public class HazelcastIMDGSpanConsumer implements SpanConsumer {

	private final HazelcastInstance hazelcastInstance;

	private final MultiMap<String, String> servicesMultiMap;
	private final IMap<HazelcastIMDGSpanKey, Span> spansMap;

	public HazelcastIMDGSpanConsumer(HazelcastInstance arg0) {
		this.hazelcastInstance = arg0;

		this.servicesMultiMap =
				this.hazelcastInstance.getMultiMap(HazelcastIMDGConstants.MULTIMAP_NAME__ZIPKIN_SERVICES);
		this.spansMap =
				this.hazelcastInstance.getMap(HazelcastIMDGConstants.IMAP_NAME__ZIPKIN_SPANS);
	}

	@Override
	public void accept(List<Span> spansList) {
		if (spansList==null || spansList.isEmpty()) {
			return;
		}

		for (int i=0 ; i<spansList.size() ; i++) {
			
			// Clone, timestamp field should not be null
		    Span tmpSpan = spansList.get(i);
		    Span span = tmpSpan.toBuilder()
		    		.timestamp(guessTimestamp(tmpSpan))
		    		.build();
		    
	        for (String serviceName : span.serviceNames()) {
	        	String effectiveServiceName = serviceName.toLowerCase().trim();
	        	String spanName = span.name;
	        	if (effectiveServiceName.length()>0 && spanName!=null && spanName.length()>0) {
	        		spanName = spanName.toLowerCase().trim();
	        		if (!servicesMultiMap.containsEntry(effectiveServiceName, spanName)) {
		        		servicesMultiMap.put(effectiveServiceName, spanName);
	        		}
	        	}
	        }
	        
	        HazelcastIMDGSpanKey spanKey = this._getKey(span);
	        
	        // Insert or update
	        Span previous = spansMap.putIfAbsent(spanKey, span);
	        if (previous!=null) {
	        	Span merged = previous.toBuilder().merge(span).build();
	        	spansMap.put(spanKey, merged);
	        }


		}
	    
	}

	private HazelcastIMDGSpanKey _getKey(Span span) {
		return new HazelcastIMDGSpanKey(span.traceIdHigh, span.traceId, span.id);
	}

}
