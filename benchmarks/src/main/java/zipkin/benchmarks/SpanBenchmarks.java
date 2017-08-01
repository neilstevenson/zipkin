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
package zipkin.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.TraceKeys;
import zipkin.internal.Span2;
import zipkin.internal.Util;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class SpanBenchmarks {
  // endpoints are almost always cached, so caching here to record more accurate performance
  static final Endpoint web = Endpoint.create("web", 124 << 24 | 13 << 16 | 90 << 8 | 3);
  static final Endpoint app =
      Endpoint.builder().serviceName("app").ipv4(172 << 24 | 17 << 16 | 2).port(8080).build();

  final Span.Builder sharedBuilder;
  final Span2.Builder shared2Builder;

  public SpanBenchmarks() {
    sharedBuilder = buildClientOnlySpan(Span.builder()).toBuilder();
    shared2Builder = buildClientOnlySpan2().toBuilder();
  }

  @Benchmark
  public Span buildLocalSpan() {
    return Span.builder()
        .traceId(1L)
        .id(1L)
        .name("work")
        .timestamp(1444438900948000L)
        .duration(31000L)
        .addBinaryAnnotation(BinaryAnnotation.create(Constants.LOCAL_COMPONENT, "worker", app))
        .build();
  }

  static final long traceId = Util.lowerHexToUnsignedLong("86154a4ba6e91385");
  static final long spanId = Util.lowerHexToUnsignedLong("4d1e00c0db9010db");
  static final Endpoint frontend = Endpoint.create("frontend", 127 << 24 | 1);
  static final Endpoint backend = Endpoint.builder()
    .serviceName("backend")
    .ipv4(192 << 24 | 168 << 16 | 99 << 8 | 101)
    .port(9000)
    .build();

  @Benchmark
  public Span buildClientOnlySpan() {
    return buildClientOnlySpan(Span.builder());
  }

  static Span buildClientOnlySpan(Span.Builder builder) {
    return builder
      .traceId(traceId)
      .parentId(traceId)
      .id(spanId)
      .name("get")
      .timestamp(1472470996199000L)
      .duration(207000L)
      .addAnnotation(Annotation.create(1472470996199000L, Constants.CLIENT_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996238000L, Constants.WIRE_SEND, frontend))
      .addAnnotation(Annotation.create(1472470996403000L, Constants.WIRE_RECV, frontend))
      .addAnnotation(Annotation.create(1472470996406000L, Constants.CLIENT_RECV, frontend))
      .addBinaryAnnotation(BinaryAnnotation.create(TraceKeys.HTTP_PATH, "/api", frontend))
      .addBinaryAnnotation(BinaryAnnotation.create("clnt/finagle.version", "6.45.0", frontend))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.SERVER_ADDR, backend))
      .build();
  }

  @Benchmark
  public Span buildClientOnlySpan_clear() {
    return buildClientOnlySpan(sharedBuilder.clear());
  }

  @Benchmark
  public Span2 buildClientOnlySpan2() {
    return buildClientOnlySpan2(Span2.builder());
  }

  static Span2 buildClientOnlySpan2(Span2.Builder builder) {
    return builder
      .traceId(traceId)
      .parentId(traceId)
      .id(spanId)
      .name("get")
      .kind(Span2.Kind.CLIENT)
      .localEndpoint(frontend)
      .remoteEndpoint(backend)
      .timestamp(1472470996199000L)
      .duration(207000L)
      .addAnnotation(1472470996238000L, Constants.WIRE_SEND)
      .addAnnotation(1472470996403000L, Constants.WIRE_RECV)
      .putTag(TraceKeys.HTTP_PATH, "/api")
      .putTag("clnt/finagle.version", "6.45.0")
      .build();
  }

  @Benchmark
  public Span2 buildClientOnlySpan2_clear() {
    return buildClientOnlySpan2(shared2Builder.clear());
  }

  @Benchmark
  public Span2 buildClientOnlySpan2_clone() {
    return shared2Builder.clone().build();
  }

  @Benchmark
  public Span buildRpcSpan() {
    return Span.builder() // web calls app
        .traceId(1L)
        .id(2L)
        .parentId(1L)
        .name("get")
        .timestamp(1444438900941000L)
        .duration(77000L)
        .addAnnotation(Annotation.create(1444438900941000L, Constants.CLIENT_SEND, web))
        .addAnnotation(Annotation.create(1444438900947000L, Constants.SERVER_RECV, app))
        .addAnnotation(Annotation.create(1444438901017000L, Constants.SERVER_SEND, app))
        .addAnnotation(Annotation.create(1444438901018000L, Constants.CLIENT_RECV, web))
        .addBinaryAnnotation(BinaryAnnotation.address(Constants.SERVER_ADDR, app))
        .addBinaryAnnotation(BinaryAnnotation.address(Constants.CLIENT_ADDR, web))
        .build();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + SpanBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
