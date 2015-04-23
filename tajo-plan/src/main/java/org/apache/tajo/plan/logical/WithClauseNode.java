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

/**
 * 
 */
package org.apache.tajo.plan.logical;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Arrays;

public class WithClauseNode extends LogicalNode {
  @Expose @SerializedName("Query")
  private ArrayList<Expr> withClauseList;
  @Expose @SerializedName("tableName")
  private ArrayList<String> tableNameList;
  @Expose @SerializedName("Node")
  private ArrayList<LogicalNode> logicalNodeList;


  public WithClauseNode(int pid) {
    super(pid, NodeType.WITH_CLAUSE);
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(withClauseList, tableNameList, logicalNodeList);
  }

  @Override
  public boolean equals(Object object) {
    if(object instanceof WithClauseNode) {
      return this.withClauseList.equals(((WithClauseNode) object).getWithClause())
          && this.tableNameList.equals(((WithClauseNode) object).getTableName())
          && this.tableNameList.equals(((WithClauseNode) object).getTableName());
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ...
    return null;
  }

  @Override
  public String toJson() {
    return PlanGsonHelper.toJson(this, LogicalNode.class);
  }

  public int childNum() {
    return 1;
  }

  public LogicalNode getChild(int idx) {
    return logicalNodeList.get(idx);
  }

  public void preOrder(LogicalNodeVisitor visitor) {
    return;
  }
  public void postOrder(LogicalNodeVisitor visitor) {
    return;
  }
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);
    planStr.appendappendTitle(" as ").appendTitle(tableName);
    return planStr;
  }
}
