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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.projection.Projections;

import sun.misc.Unsafe;
import zipkin.Annotation;
import zipkin.Endpoint;
import zipkin.Span;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
@SuppressWarnings("restriction")
public class HazelcastIMDGSpanConsumerTest {
	
	private static Field hazelcastInstanceField;
	private static Field servicesMultiMapField;
	private static Field spansMapField;
	private static Unsafe unsafe;

	private static HazelcastInstance hazelcastInstance;
	private static HazelcastIMDGSpanConsumer hazelcastIMDGSpanConsumer;
	
	private static Span span1, span2, span3, span4, span5;

	private static MultiMap<String, String> servicesMultiMap;
	private static IMap<HazelcastIMDGSpanKey, Span> spansMap;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// Ensure no remnants from other tests
		Hazelcast.shutdownAll();

		// Create a standalone Hazelcast
		Config config = new Config();
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		servicesMultiMap = hazelcastInstance.getMultiMap(HazelcastIMDGConstants.MULTIMAP_NAME__ZIPKIN_SERVICES);
		spansMap = hazelcastInstance.getMap(HazelcastIMDGConstants.IMAP_NAME__ZIPKIN_SPANS);
		
		// Prepare for class construction without constructors
		Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
		unsafeField.setAccessible(true);
		unsafe = (Unsafe) unsafeField.get(null);
		
		hazelcastInstanceField = HazelcastIMDGSpanConsumer.class.getDeclaredField("hazelcastInstance");
		hazelcastInstanceField.setAccessible(true);
		
		servicesMultiMapField = HazelcastIMDGSpanConsumer.class.getDeclaredField("servicesMultiMap");
		servicesMultiMapField.setAccessible(true);
		
		spansMapField = HazelcastIMDGSpanConsumer.class.getDeclaredField("spansMap");
		spansMapField.setAccessible(true);

		hazelcastIMDGSpanConsumer = (HazelcastIMDGSpanConsumer) unsafe.allocateInstance(HazelcastIMDGSpanConsumer.class);
		
		hazelcastInstanceField.set(hazelcastIMDGSpanConsumer, hazelcastInstance);
		servicesMultiMapField.set(hazelcastIMDGSpanConsumer, servicesMultiMap);
		spansMapField.set(hazelcastIMDGSpanConsumer, spansMap);
	}

	// Test data
	@BeforeClass
	public static void setUpBeforeClass2() throws Exception {
		Endpoint fooEndpoint = Endpoint.builder().serviceName("foo").build();
		Endpoint barEndpoint = Endpoint.builder().serviceName("bar").build();
		Endpoint bazEndpoint = Endpoint.builder().serviceName("baz").build();
		
		Annotation foo = Annotation.builder().value("foo").timestamp(1L).endpoint(fooEndpoint).build();
		Annotation bar = Annotation.builder().value("bar").timestamp(2L).endpoint(barEndpoint).build();
		Annotation baz = Annotation.builder().value("baz").timestamp(3L).endpoint(bazEndpoint).build();
		
		span1 = Span.builder()
			    .traceId(1)
			    .id(1)
			    .name("span1")
			    .timestamp(100L)
			    .build();
		span2 = Span.builder()
			    .traceId(2)
			    .id(2)
			    .name("span2")
			    .timestamp(200L)
			    .build();
		// Span 3 has two services
		span3 = Span.builder()
			    .traceId(3)
			    .id(3)
			    .name("span3")
			    .timestamp(300L)
			    .addAnnotation(foo)
			    .addAnnotation(bar)
			    .build();
		// Span 4 has one service
		span4 = Span.builder()
			    .traceId(4)
			    .id(4)
			    .name("span4")
			    .timestamp(400L)
			    .addAnnotation(baz)
			    .build();
		// Span 5 has one service, one of the same ones as span 3
		span5 = Span.builder()
			    .traceId(5)
			    .id(5)
			    .name("span5")
			    .timestamp(500L)
			    .addAnnotation(foo)
			    .build();
	}

	@Before
	public void setUp() throws Exception {
		servicesMultiMap.clear();
		spansMap.clear();
	}

	@Test
	public void test_null() {
		hazelcastIMDGSpanConsumer.accept(null);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(0));
		assertThat("SpansMap", spansMap.size(), equalTo(0));
	}

	@Test
	public void test_empty() {
		List<Span> input = new ArrayList<>();
		
		hazelcastIMDGSpanConsumer.accept(input);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(0));
		assertThat("SpansMap", spansMap.size(), equalTo(0));
	}

	@Test
	public void test_one_span() {
		List<Span> input = new ArrayList<>();
		input.add(span1);
		
		hazelcastIMDGSpanConsumer.accept(input);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(0));
		assertThat("SpansMap", spansMap.size(), equalTo(1));
		
		Collection<Long> spanIds = spansMap.project(Projections.singleAttribute("id"));

		assertThat(spanIds, hasItems(1L));
	}

	@Test
	public void test_two_spans() {
		List<Span> input = new ArrayList<>();
		input.add(span1);
		input.add(span2);
		
		hazelcastIMDGSpanConsumer.accept(input);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(0));
		assertThat("SpansMap", spansMap.size(), equalTo(2));
		
		Collection<Long> spanIds = spansMap.project(Projections.singleAttribute("id"));

		assertThat(spanIds, hasItems(1L, 2L));
	}

	@Test
	public void test_four_spans() {
		List<Span> input = new ArrayList<>();
		input.add(span1);
		input.add(span2);
		input.add(span3);
		input.add(span4);
		
		hazelcastIMDGSpanConsumer.accept(input);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(3));
		assertThat("SpansMap", spansMap.size(), equalTo(4));
		
		Collection<Long> spanIds = spansMap.project(Projections.singleAttribute("id"));

		assertThat(spanIds, hasItems(1L, 2L, 3L, 4L));
	}

	@Test
	public void test_five_spans() {
		List<Span> input = new ArrayList<>();
		input.add(span1);
		input.add(span2);
		input.add(span3);
		input.add(span4);
		input.add(span5);
		
		hazelcastIMDGSpanConsumer.accept(input);

		assertThat("ServicesMultiMap", servicesMultiMap.keySet().size(), equalTo(3));
		assertThat("SpansMap", spansMap.size(), equalTo(5));
		
		Collection<Long> spanIds = spansMap.project(Projections.singleAttribute("id"));

		assertThat(spanIds, hasItems(1L, 2L, 3L, 4L, 5L));
	}

}
