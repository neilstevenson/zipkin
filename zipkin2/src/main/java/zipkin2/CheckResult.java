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
package zipkin2;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 * Answers the question: Are operations on this component likely to succeed?
 *
 * <p>Implementations should initialize the component if necessary. It should test a remote
 * connection, or consult a trusted source to derive the result. They should use least resources
 * possible to establish a meaningful result, and be safe to call many times, even concurrently.
 *
 * @see CheckResult#OK
 */
@Immutable
@AutoValue
public abstract class CheckResult {
  public static final CheckResult OK = new AutoValue_CheckResult(true, null);

  public static CheckResult failed(Throwable error) {
    return new AutoValue_CheckResult(false, error);
  }

  public abstract boolean ok();

  /** Present when not ok */
  @Nullable public abstract Throwable error();

  CheckResult() {
  }
}
