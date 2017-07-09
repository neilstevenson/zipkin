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
package io.zipkin.java;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <P>Start a Spring Boot main program, and let Spring Boot do the
 * rest.
 * </P>
 * <P>Specifically, if Spring Boot finds a {@code hazelcast.xml}
 * file and Hazelcast jars on the classpath, it'll assume we
 * want a Hazelcast server instance and build one for us.
 * </P>
 * <P>If you wish to access it, the Hazelcast instance created
 * would be available for autowiring as a @Bean of type
 * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}.
 * </P>
 */
@SpringBootApplication
public class ZipkinHazelcastIMDGServer {

	public static void main(String[] args) {
		SpringApplication.run(ZipkinHazelcastIMDGServer.class, args);
	}
}
