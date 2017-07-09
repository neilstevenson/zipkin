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

public class HazelcastIMDGConstants {

	// Spans are stored in an IMap<HazelcastIMDGSpanKey, Span>
	public static final String IMAP_NAME__ZIPKIN_SPANS =
			"zipkin_spans";

	
	// Service name to span names, 1:many
	public static final String MULTIMAP_NAME__ZIPKIN_SERVICES =
			"zipkin_services";

}
