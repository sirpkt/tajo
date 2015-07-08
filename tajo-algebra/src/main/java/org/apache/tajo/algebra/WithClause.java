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

import java.util.ArrayList;

public class WithClause extends Expr {
  @Expose @SerializedName("Query")
  private Expr[] withClauseList;
  @Expose @SerializedName("subPlan")
  private Expr subquery;

  public WithClause(Expr[] withClauseList, Expr subquery) {
    super(OpType.With);
    this.withClauseList = withClauseList;
    this.subquery = subquery;
  }

  public Expr getSubquery() {
    return subquery;
  }

  public void setSubquery(Expr subquery) {
    this.subquery = subquery;
  }

  public Expr[] getWithClause() {
    return this.withClauseList;
  }

  public void setWithClause(Expr[] withClauseList) {
    this.withClauseList=withClauseList;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(withClauseList);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    if(expr instanceof WithClause) {
      return this.withClauseList.equals(((WithClause) expr).getWithClause())
          && this.subquery.equals(((WithClause) expr).getSubquery());
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    WithClause with_clause = (WithClause) super.clone();
    with_clause.withClauseList = withClauseList;
    return with_clause;
  }
}
