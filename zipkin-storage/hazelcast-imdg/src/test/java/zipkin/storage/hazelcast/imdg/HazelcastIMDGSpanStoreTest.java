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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import zipkin.Span;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
public class HazelcastIMDGSpanStoreTest {
	private static final boolean DEFAULT_STRICT_TRACE_ID = true;

	private static HazelcastInstance hazelcastInstance;
	private static HazelcastIMDGSpanStore hazelcastIMDGSpanStore;
	private static IMap<HazelcastIMDGSpanKey, Span> spans;

	/**
	 * <P>Create a Hazelcast <U><I>server</I></U> instead of a <U><I>client</I></U>
	 * for testing. They'll behave the same except if we use a server we don't have
	 * concerns around start up sequencing (server needs to be ready before the client
	 * tries to connect), network, port clashes, etc.
	 * </P>
	 * <P>Additionally, if we wish to test indexing, this is where we do it.
	 * </P>
	 *
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// Ensure no remnants from other tests
		Hazelcast.shutdownAll();

		Config config = new Config();
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		
		/* If we want to prove that an index can be added to a map for faster
		 * retrieval do so here. Timing measurements won't show much difference
		 * unless data volumes are significant, which is inappropriate for a unit
		 * or simple test.
		 */
		MapConfig spanMapConfig = new MapConfig();
		//spanMapConfig.addMapIndexConfig(new MapIndexConfig("field_to_index", false));
		config.getMapConfigs().put(HazelcastIMDGConstants.IMAP_NAME__ZIPKIN_SPANS, spanMapConfig);
		
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);

		spans = hazelcastInstance.getMap(HazelcastIMDGConstants.IMAP_NAME__ZIPKIN_SPANS);
		
		hazelcastIMDGSpanStore = new HazelcastIMDGSpanStore(DEFAULT_STRICT_TRACE_ID, hazelcastInstance);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		hazelcastInstance.shutdown();
	}

	@Before
	public void setUp() throws Exception {
		hazelcastInstance.getDistributedObjects()
		.stream()
			.filter(MultiMap.class::isInstance)
			.map(MultiMap.class::cast)
			.forEach(multiMap -> multiMap.clear());
		hazelcastInstance.getDistributedObjects()
		.stream()
			.filter(IMap.class::isInstance)
			.map(IMap.class::cast)
			.forEach(imap -> imap.clear());
	}

	/* Spans are sorted by timestamp, ascending
	 */
	@Test
	public void test_getTrace_sorting() {
		// Span4 first, Span2 & Span3 at same time, then Span1
		Span span4 = Span.builder()
			    .traceId(1)
			    .id(4)
			    .name("span4")
			    .timestamp(100L)
			    .build();
		Span span2 = Span.builder()
			    .traceId(1)
			    .id(2)
			    .name("span2")
			    .timestamp(200L)
			    .build();
		Span span3 = Span.builder()
			    .traceId(1)
			    .id(3)
			    .name("span3")
			    .timestamp(200L)
			    .build();
		Span span1 = Span.builder()
			    .traceId(1)
			    .id(1)
			    .name("span1")
			    .timestamp(300L)
			    .build();
		
		Span[] data = new Span[] { span1, span2, span3, span4 };
		
		for (Span value : data) {
			HazelcastIMDGSpanKey key =
					  new HazelcastIMDGSpanKey(value.traceIdHigh, value.traceId, value.id);
			spans.put(key, value);
		}

		List<Span> result = hazelcastIMDGSpanStore.getTrace(0L, 1L);

		assertThat(result, not(nullValue()));
		assertThat(result, is(instanceOf(List.class)));
		assertThat(result.size(), equalTo(4));

		assertThat("Span 4 earliest", result.get(0).id, equalTo(4L));
		
		assertThat("Span 2 & 3 could be second",
				result.get(1).id, anyOf(equalTo(2L), equalTo(3L))
				);
		assertThat("Span 2 & 3 could be third",
				result.get(2).id, anyOf(equalTo(2L), equalTo(3L))
				);
		assertThat("Second and third equal not same span",
				result.get(1).id, not(equalTo(result.get(2).id))
				);

		assertThat("Span 1 last", result.get(3).id, equalTo(1L));
	}

	/* Confirm traceIdHigh is used for strict mode, usnig the main span store
	 * for this class compared against a temporary one for this method.
	 */
	@Test
	public void test_getTrace_strict_or_not_strict() {
		Span span1 = Span.builder()
				.traceIdHigh(10)
			    .traceId(20)
			    .id(1)
			    .name("span1")
			    .timestamp(3L)
			    .build();
		Span span2 = Span.builder()
				.traceIdHigh(20)
			    .traceId(20)
			    .id(2)
			    .name("span2")
			    .timestamp(2L)
			    .build();
		Span span3 = Span.builder()
				.traceIdHigh(30)
			    .traceId(30)
			    .id(3)
			    .name("span3")
			    .timestamp(1L)
			    .build();
		
		Span[] data = new Span[] { span1, span2, span3 };
		
		for (Span value : data) {
			HazelcastIMDGSpanKey key =
					  new HazelcastIMDGSpanKey(value.traceIdHigh, value.traceId, value.id);
			spans.put(key, value);
		}

		List<Span> resultStrict = hazelcastIMDGSpanStore.getTrace(10L, 20L);
		
		assertThat("resultStrict", resultStrict, not(nullValue()));
		assertThat("resultStrict", resultStrict, is(instanceOf(List.class)));
		assertThat("resultStrict", resultStrict.size(), equalTo(1));

		spans.clear();

		HazelcastIMDGSpanStore notStrictHazelcastIMDGSpanStore 
			= new HazelcastIMDGSpanStore(!DEFAULT_STRICT_TRACE_ID, hazelcastInstance);
		
		for (Span value : data) {
			HazelcastIMDGSpanKey key =
					  new HazelcastIMDGSpanKey(value.traceIdHigh, value.traceId, value.id);
			spans.put(key, value);
		}

		List<Span> resultNotStrict = notStrictHazelcastIMDGSpanStore.getTrace(10L, 20L);
		
		assertThat("resultNotStrict", resultNotStrict, not(nullValue()));
		assertThat("resultNotStrict", resultNotStrict, is(instanceOf(List.class)));
		assertThat("resultNotStrict", resultNotStrict.size(), equalTo(2));

		assertThat("Span 2 earliest", resultNotStrict.get(0).id, equalTo(2L));
		assertThat("Span 1 latest", resultNotStrict.get(1).id, equalTo(1L));

	}

}
