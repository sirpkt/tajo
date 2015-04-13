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
  private ArrayList<Expr> withClauseList;
  @Expose @SerializedName("tableName")
  private ArrayList<String> tableNameList;

  public WithClause(ArrayList<Expr> withClauseList, ArrayList<String> tableNameList) {
    super(OpType.With);
    this.withClauseList = withClauseList;
    this.tableNameList = tableNameList;
  }

  public ArrayList<Expr> getWithClause() {
    return this.withClauseList;
  }

  public void setWithClause(ArrayList<Expr> withClauseList) {
    this.withClauseList=withClauseList;
  }

  public ArrayList<String > getTableName() {
    return this.tableNameList;
  }

  public void setTableName(ArrayList<String> tableNameList) {
    this.tableNameList=tableNameList;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(withClauseList, tableNameList);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    if(expr instanceof WithClause) {
      return this.withClauseList.equals(((WithClause) expr).getWithClause())
          && this.tableNameList.equals(((WithClause) expr).getTableName());
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    WithClause with_clause = (WithClause) super.clone();
    with_clause.withClauseList = withClauseList;
    with_clause.tableNameList = tableNameList;
    return with_clause;
  }
}
