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
import java.util.Set;

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
		    		    
	        HazelcastIMDGSpanKey spanKey = this._getKey(span);
	        
	        // Insert or update
	        Span previous = spansMap.putIfAbsent(spanKey, span);
	        if (previous!=null) {
	        	Span merged = previous.toBuilder().merge(span).build();
	        	spansMap.put(spanKey, merged);
	        }

	        // Service names on span and parents
	        String theSpanName = span.name;
        	Long parentId = span.parentId;
        	this._recordServices(theSpanName, span.serviceNames());
        	while (parentId!=null) {
        		HazelcastIMDGSpanKey parentKey
        		= new HazelcastIMDGSpanKey(span.traceIdHigh, span.traceId, parentId);
        		
        		Span parentSpan = spansMap.get(parentKey);
        		
        		if (parentSpan!=null) {
                	this._recordServices(theSpanName, parentSpan.serviceNames());
        		}
        		
        		parentId = (parentSpan==null ? null : parentSpan.parentId);
        	}
		}

	}

	private void _recordServices(String spanName, Set<String> serviceNames) {
		if (spanName==null) {
			return;
		}
		String effectiveSpanName = spanName.toLowerCase().trim();
		if (effectiveSpanName.length()==0) {
			return;
		}
		
        for (String serviceName : serviceNames) {
        	if (serviceName!=null) {
            	String effectiveServiceName = serviceName.toLowerCase().trim();
            	if (effectiveServiceName.length()>0 && spanName.length()>0) {
            		if (!servicesMultiMap.containsEntry(effectiveServiceName, effectiveSpanName)) {
    	        		servicesMultiMap.put(effectiveServiceName, effectiveSpanName);
            		}
            	}
        	}
        }

	}

	private HazelcastIMDGSpanKey _getKey(Span span) {
		return new HazelcastIMDGSpanKey(span.traceIdHigh, span.traceId, span.id);
	}

}
