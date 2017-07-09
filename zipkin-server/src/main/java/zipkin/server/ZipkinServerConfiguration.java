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
package zipkin.server;

import com.github.kristofa.brave.Brave;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.actuate.health.HealthAggregator;
import org.springframework.boot.actuate.metrics.buffer.CounterBuffers;
import org.springframework.boot.actuate.metrics.buffer.GaugeBuffers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.server.brave.TracedStorageComponent;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

@Configuration
public class ZipkinServerConfiguration {

  /** Registers health for any components, even those not in this jar. */
  @Bean ZipkinHealthIndicator zipkinHealthIndicator(HealthAggregator healthAggregator) {
    return new ZipkinHealthIndicator(healthAggregator);
  }

  @Bean
  @ConditionalOnMissingBean(CollectorSampler.class)
  CollectorSampler traceIdSampler(@Value("${zipkin.collector.sample-rate:1.0}") float rate) {
    return CollectorSampler.create(rate);
  }

  @Bean
  @ConditionalOnMissingBean(CollectorMetrics.class)
  CollectorMetrics metrics(Optional<CounterBuffers> counterBuffers, Optional<GaugeBuffers> gaugeBuffers) {
    // it is not guaranteed that BufferCounterService/CounterBuffers will be used,
    // for ex., com.datastax.cassandra:cassandra-driver-core brings com.codahale.metrics.MetricRegistry
    // and as result DropwizardMetricServices is getting instantiated instead of standard Java8 BufferCounterService.
    // On top of it Cassandra driver heavily relies on Dropwizard metrics and manually excluding it from pom.xml is not an option.
    // MetricsDropwizardAutoConfiguration can be manually excluded either, as Cassandra metrics won't be recorded.
    return new ActuateCollectorMetrics(counterBuffers.orElse(new CounterBuffers()),
                                       gaugeBuffers.orElse(new GaugeBuffers()));
  }

  @Configuration
  @ConditionalOnSelfTracing
  static class BraveTracedStorageComponentEnhancer implements BeanPostProcessor {

    @Autowired(required = false)
    Brave brave;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
      return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
      if (bean instanceof StorageComponent && brave != null) {
        return new TracedStorageComponent(brave, (StorageComponent) bean);
      }
      return bean;
    }
  }

  @Configuration
  // "matchIfMissing = true" ensures this is used when there's no configured storage type
  @ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "mem", matchIfMissing = true)
  @ConditionalOnMissingBean(StorageComponent.class)
  static class InMemoryConfiguration {
    @Bean StorageComponent storage(
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
      @Value("${zipkin.storage.mem.max-spans:500000}") int maxSpans) {
      return InMemoryStorage.builder().strictTraceId(strictTraceId).maxSpanCount(maxSpans).build();
    }
  }
}
