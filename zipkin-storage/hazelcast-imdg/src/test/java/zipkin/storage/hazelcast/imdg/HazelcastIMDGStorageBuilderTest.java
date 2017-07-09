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
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;

/**
 * <P>Test builder sets up fields. Can't test {@code build()} as
 * this requires client-server connection.
 * </P>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
public class HazelcastIMDGStorageBuilderTest {

	private static final boolean DEFAULT_STRICT_TRACE_ID = true;
	private static Field clientConfigField;
	private static Field executorField;
	private static Field strictTraceIdField;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// Ensure no remnants from other tests
		Hazelcast.shutdownAll();
		
		clientConfigField = HazelcastIMDGStorageBuilder.class.getDeclaredField("clientConfig");
		clientConfigField.setAccessible(true);
		
		executorField = HazelcastIMDGStorageBuilder.class.getDeclaredField("executor");
		executorField.setAccessible(true);
		
		strictTraceIdField = HazelcastIMDGStorageBuilder.class.getDeclaredField("strictTraceId");
		strictTraceIdField.setAccessible(true);
	}
	
	@Test
	public void noFieldsSet() throws Exception {
		HazelcastIMDGStorageBuilder hazelcastIMDGStorageBuilder
			= new HazelcastIMDGStorageBuilder();

		Object clientConfig = clientConfigField.get(hazelcastIMDGStorageBuilder);
		
		Object executor = executorField.get(hazelcastIMDGStorageBuilder);

		Object strictTraceId = strictTraceIdField.get(hazelcastIMDGStorageBuilder);
		
		assertThat("clientConfig", clientConfig, nullValue());
		assertThat("executor", executor, nullValue());
		assertThat("strictTraceId", strictTraceId, is(equalTo(DEFAULT_STRICT_TRACE_ID)));
	}

	@Test
	public void allFieldsSet() throws Exception {
		HazelcastIMDGStorageBuilder hazelcastIMDGStorageBuilder
			= new HazelcastIMDGStorageBuilder();

		hazelcastIMDGStorageBuilder.clientConfig(new ClientConfig());
		hazelcastIMDGStorageBuilder.executor(Executors.newSingleThreadExecutor());
		hazelcastIMDGStorageBuilder.strictTraceId(!DEFAULT_STRICT_TRACE_ID);
		
		Object clientConfig = clientConfigField.get(hazelcastIMDGStorageBuilder);
		
		Object executor = executorField.get(hazelcastIMDGStorageBuilder);

		Object strictTraceId = strictTraceIdField.get(hazelcastIMDGStorageBuilder);
	
		assertThat("clientConfig", clientConfig, not(nullValue()));
		assertThat("executor", executor, not(nullValue()));
		assertThat("strictTraceId", strictTraceId, is(not(equalTo(DEFAULT_STRICT_TRACE_ID))));
	}
}
