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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class WithClause extends Expr {
  @Expose @SerializedName("Query")
  private String withClause;
  @Expose @SerializedName("tableName")
  private String tableName;

  public WithClause(String withClause, String tableName) {
    super(OpType.With);
    this.withClause = new String(withClause);
    this.tableName = new String(tableName);
  }

  public String getWithClause() {
    return this.withClause;
  }

  public void setWithClause(String withClause) {
    this.withClause=withClause;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName=tableName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(withClause, tableName);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    if (expr instanceof WithClause) {
      WithClause other = (WithClause) expr;
      return this.withClause.equals(((WithClause) expr).getWithClause())
          && this.tableName.equals(((WithClause) expr).getTableName());
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    WithClause with_clause = (WithClause) super.clone();
    with_clause.withClause = withClause;
    with_clause.tableName = tableName;
    return with_clause;
  }
}
