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

import java.io.Serializable;

/**
 * <P>Key of a {@link zipkin.Span Span} when stored in Hazelcast.
 * </P>
 */
public class HazelcastIMDGSpanKey implements Serializable {
	
	public HazelcastIMDGSpanKey() {
	}
	
	public HazelcastIMDGSpanKey(long traceIdHigh, long traceIdLow, long spanId) {
		this.traceIdHigh = traceIdHigh;
		this.traceIdLow = traceIdLow;
		this.spanId = spanId;
	}

	private static final long serialVersionUID = 0L;

	private long traceIdHigh;
	private long traceIdLow;
	private long spanId;
	
	public long getTraceIdHigh() {
		return traceIdHigh;
	}

	public void setTraceIdHigh(long traceIdHigh) {
		this.traceIdHigh = traceIdHigh;
	}

	public long getTraceIdLow() {
		return traceIdLow;
	}

	public void setTraceIdLow(long traceIdLow) {
		this.traceIdLow = traceIdLow;
	}

	public long getSpanId() {
		return spanId;
	}

	public void setSpanId(long spanId) {
		this.spanId = spanId;
	}

	@Override
	public String toString() {
		return "HazelcastIMDGSpanKey [traceIdHigh=" + traceIdHigh + ", traceIdLow=" + traceIdLow + ", spanId=" + spanId
				+ "]";
	}
	  
}
