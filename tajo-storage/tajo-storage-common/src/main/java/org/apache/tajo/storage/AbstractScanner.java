/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;

import java.io.IOException;

/**
 * It's a dummy class to avoid subclass to implement all methods.
 */
public abstract class AbstractScanner implements Scanner {

  @Override
  public void init() throws IOException {
    throw new UnimplementedException();
  }

  @Override
  public void reset() throws IOException {
    throw new UnimplementedException();
  }

  @Override
  public void close() throws IOException {
    throw new UnimplementedException();
  }

  @Override
  public boolean isProjectable() {
    throw new UnimplementedException();
  }

  @Override
  public void setTarget(Column[] targets) {
    throw new UnsupportedException();
  }

  @Override
  public boolean isSelectable() {
    throw new UnimplementedException();
  }

  @Override
  public void setFilter(EvalNode filter) {
    throw new UnsupportedException();
  }

  @Override
  public boolean isSplittable() {
    throw new UnimplementedException();
  }

  @Override
  public float getProgress() {
    throw new UnimplementedException();
  }

  @Override
  public TableStats getInputStats() {
    throw new UnimplementedException();
  }

  @Override
  public Schema getSchema() {
    throw new UnimplementedException();
  }
}
