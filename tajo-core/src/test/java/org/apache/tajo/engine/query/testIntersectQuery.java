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

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

/*
 * Notations
 * - S - select
 * - SA - select *
 * - I - intersect
 * - G - group by
 * - O - order by
 */
public class TestIntersectQuery extends QueryTestCaseBase {
  public TestIntersectQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  /**
   * S (SA I SA) O
   */
  public final void testIntersect1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (S I S) O
   */
  public final void testIntersect2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S O ((S G) I (S G))
   */
  public final void testIntersect3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S G)
   */
  public final void testIntersect4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S F G)
   */
  public final void testIntersect5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (SA)
   */
  public final void testIntersect6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (SA)
   */
  public final void testIntersect7() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect8() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect9() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect10() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect11() throws Exception {
    // test filter pushdown
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect12() throws Exception {
    // test filter pushdown
    // with stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect13() throws Exception {
    // test filter pushdown
    // with stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect14() throws Exception {
    // test filter pushdown
    // with group by stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect15() throws Exception {
    // test filter pushdown
    // with group by out of intersect query and join in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersect16() throws Exception {
    // test filter pushdown
    // with count distinct out of intersect query and join in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (SA I SA) O
   */
  public final void testIntersectAll1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (S I S) O
   */
  public final void testIntersectAll2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S O ((S G) I (S G))
   */
  public final void testIntersectAll3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S G)
   */
  public final void testIntersectAll4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S F G)
   */
  public final void testIntersectAll5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (SA)
   */
  public final void testIntersectAll6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (SA)
   */
  public final void testIntersectAll7() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll8() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll9() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll10() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll11() throws Exception {
    // test filter pushdown
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll12() throws Exception {
    // test filter pushdown
    // with stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll13() throws Exception {
    // test filter pushdown
    // with stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll14() throws Exception {
    // test filter pushdown
    // with group by stage in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll15() throws Exception {
    // test filter pushdown
    // with group by out of intersect query and join in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIntersectAll16() throws Exception {
    // test filter pushdown
    // with count distinct out of intersect query and join in intersect query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}