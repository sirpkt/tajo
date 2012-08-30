/**
 * 
 */
package tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.conf.TajoConf;
import tajo.SubqueryContext;
import tajo.engine.exception.InternalException;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.logical.*;
import tajo.engine.planner.physical.*;
import tajo.index.IndexUtil;
import tajo.storage.StorageManager;

import java.io.IOException;

/**
 * This class generates a physical execution plan.
 * 
 * @author Hyunsik Choi
 * 
 */
public class PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlanner.class);
  private final StorageManager sm;
  private final TajoConf conf;

  public PhysicalPlanner(TajoConf conf, StorageManager sm) {
    this.conf = conf;
    this.sm = sm;
  }

  public PhysicalExec createPlan(SubqueryContext ctx, LogicalNode logicalPlan)
      throws InternalException {
    PhysicalExec plan;
    try {
      plan = createPlanRecursive(ctx, logicalPlan);
    } catch (IOException ioe) {
      throw new InternalException(ioe);
    }

    return plan;
  }

  private PhysicalExec createPlanRecursive(SubqueryContext ctx,
      LogicalNode logicalNode) throws IOException {
    PhysicalExec outer;
    PhysicalExec inner;

    switch (logicalNode.getType()) {
    case ROOT:
      LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
      return createPlanRecursive(ctx, rootNode.getSubNode());

    case EXPRS:
      EvalExprNode evalExpr = (EvalExprNode) logicalNode;
      return new EvalExprExec(evalExpr);

    case STORE:
      StoreTableNode storeNode = (StoreTableNode) logicalNode;
      outer = createPlanRecursive(ctx, storeNode.getSubNode());
      return createStorePlan(ctx, storeNode, outer);

    case SELECTION:
      SelectionNode selNode = (SelectionNode) logicalNode;
      outer = createPlanRecursive(ctx, selNode.getSubNode());
      return new SelectionExec(ctx, selNode, outer);

    case PROJECTION:
      ProjectionNode prjNode = (ProjectionNode) logicalNode;
      outer = createPlanRecursive(ctx, prjNode.getSubNode());
      return new ProjectionExec(ctx, prjNode, outer);

    case SCAN:
      outer = createScanPlan(ctx, (ScanNode) logicalNode);
      return outer;

    case GROUP_BY:
      GroupbyNode grpNode = (GroupbyNode) logicalNode;
      outer = createPlanRecursive(ctx, grpNode.getSubNode());
      return createGroupByPlan(ctx, grpNode, outer);

    case SORT:
      SortNode sortNode = (SortNode) logicalNode;
      outer = createPlanRecursive(ctx, sortNode.getSubNode());
      return createSortPlan(ctx, sortNode, outer);

    case JOIN:
      JoinNode joinNode = (JoinNode) logicalNode;
      outer = createPlanRecursive(ctx, joinNode.getOuterNode());
      inner = createPlanRecursive(ctx, joinNode.getInnerNode());
      return createJoinPlan(ctx, joinNode, outer, inner);

    case UNION:
      UnionNode unionNode = (UnionNode) logicalNode;
      outer = createPlanRecursive(ctx, unionNode.getOuterNode());
      inner = createPlanRecursive(ctx, unionNode.getInnerNode());
      return new UnionExec(outer, inner);

    case CREATE_INDEX:
      IndexWriteNode createIndexNode = (IndexWriteNode) logicalNode;
      outer = createPlanRecursive(ctx, createIndexNode.getSubNode());
      return createIndexWritePlan(sm, ctx, createIndexNode, outer);

    case BST_INDEX_SCAN:
      IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
      outer = createIndexScanExec(ctx, indexScanNode);
      return outer;

    case RENAME:
    case SET_UNION:
    case SET_DIFF:
    case SET_INTERSECT:
    case INSERT_INTO:
    case SHOW_TABLE:
    case DESC_TABLE:
    case SHOW_FUNCTION:
    default:
      return null;
    }
  }

  private long estimateSizeRecursive(SubqueryContext ctx, String [] tableIds) {
    long size = 0;
    for (String tableId : tableIds) {
      Fragment[] fragments = ctx.getTables(tableId);
      for (Fragment frag : fragments) {
        size += frag.getLength();
      }
    }
    return size;
  }

  public PhysicalExec createJoinPlan(SubqueryContext ctx, JoinNode joinNode,
      PhysicalExec outer, PhysicalExec inner) {
    switch (joinNode.getJoinType()) {
    case CROSS_JOIN:
      LOG.info("The planner chooses NLJoinExec");
      return new NLJoinExec(ctx, joinNode, outer, inner);

    case INNER:
      String [] outerLineage = PlannerUtil.getLineage(joinNode.getOuterNode());
      String [] innerLineage = PlannerUtil.getLineage(joinNode.getInnerNode());
      long outerSize = estimateSizeRecursive(ctx, outerLineage);
      long innerSize = estimateSizeRecursive(ctx, innerLineage);

      final long threshold = 1048576 * 64; // 64MB

      boolean hashJoin = false;
      if (outerSize < threshold || innerSize < threshold) {
        hashJoin = true;
      }

      if (hashJoin) {
        PhysicalExec selectedOuter;
        PhysicalExec selectedInner;

        // HashJoinExec loads the inner relation to memory.
        if (outerSize <= innerSize) {
          selectedInner = outer;
          selectedOuter = inner;
        } else {
          selectedInner = inner;
          selectedOuter = outer;
        }

        LOG.info("The planner chooses HashJoinExec");
        return new HashJoinExec(ctx, joinNode, selectedOuter, selectedInner);
      }

    default:
      QueryBlock.SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
          joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
      ExternalSortExec outerSort = new ExternalSortExec(conf, ctx, sm,
          new SortNode(sortSpecs[0], outer.getSchema(), outer.getSchema()),
          outer);
      ExternalSortExec innerSort = new ExternalSortExec(conf, ctx, sm,
          new SortNode(sortSpecs[1], inner.getSchema(), inner.getSchema()),
          inner);

      LOG.info("The planner chooses MergeJoinExec");
      return new MergeJoinExec(ctx, joinNode, outerSort, innerSort,
          sortSpecs[0], sortSpecs[1]);
    }
  }

  public PhysicalExec createStorePlan(SubqueryContext ctx,
      StoreTableNode annotation, PhysicalExec subOp) throws IOException {
    if (annotation.hasPartitionKey()) {
      switch (annotation.getPartitionType()) {
      case HASH:
        return new PartitionedStoreExec(ctx, sm, annotation, subOp);

      case RANGE:
        Column[] columns = annotation.getPartitionKeys();
        QueryBlock.SortSpec specs[] = new QueryBlock.SortSpec[columns.length];
        for (int i = 0; i < columns.length; i++) {
          specs[i] = new QueryBlock.SortSpec(columns[i]);
        }
        return new IndexedStoreExec(ctx, sm, subOp,
            annotation.getInputSchema(), annotation.getInputSchema(), specs);
      }
    }
    if (annotation instanceof StoreIndexNode) {
      return new TunnelExec(annotation.getOutputSchema(), subOp);
    }

    return new StoreTableExec(ctx, sm, annotation, subOp);
  }

  public PhysicalExec createScanPlan(SubqueryContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()),
        "Error: There is no table matched to %s", scanNode.getTableId());

    Fragment[] fragments = ctx.getTables(scanNode.getTableId());
    return new SeqScanExec(sm, scanNode, fragments);
  }

  public PhysicalExec createGroupByPlan(SubqueryContext ctx,
      GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    if (grpColumns.length == 0) {
      LOG.info("The planner chooses HashAggregationExec");
      return new HashAggregateExec(ctx, groupbyNode, subOp);
    } else {
      String [] outerLineage = PlannerUtil.getLineage(groupbyNode.getSubNode());
      long estimatedSize = estimateSizeRecursive(ctx, outerLineage);
      final long threshold = 1048576 * 64;

      // if the relation size is less than the reshold,
      // the hash aggregation will be used.
      if (estimatedSize <= threshold) {
        LOG.info("The planner chooses HashAggregationExec");
        return new HashAggregateExec(ctx, groupbyNode, subOp);
      } else {
        QueryBlock.SortSpec[] specs = new QueryBlock.SortSpec[grpColumns.length];
        for (int i = 0; i < grpColumns.length; i++) {
          specs[i] = new QueryBlock.SortSpec(grpColumns[i], true, false);
        }
        SortNode sortNode = new SortNode(specs);
        sortNode.setInputSchema(subOp.getSchema());
        sortNode.setOutputSchema(subOp.getSchema());
        // SortExec sortExec = new SortExec(sortNode, child);
        ExternalSortExec sortExec = new ExternalSortExec(conf, ctx, sm, sortNode,
            subOp);
        LOG.info("The planner chooses SortAggregationExec");
        return new SortAggregateExec(ctx, groupbyNode, sortExec);
      }
    }
  }

  public PhysicalExec createSortPlan(SubqueryContext ctx, SortNode sortNode,
      PhysicalExec subOp) throws IOException {
    return new ExternalSortExec(conf, ctx, sm, sortNode, subOp);
  }

  public PhysicalExec createIndexWritePlan(StorageManager sm,
      SubqueryContext ctx, IndexWriteNode indexWriteNode, PhysicalExec subOp)
      throws IOException {

    return new IndexWriteExec(sm, indexWriteNode, ctx.getTable(indexWriteNode
        .getTableName()), subOp);
  }

  public PhysicalExec createIndexScanExec(SubqueryContext ctx,
      IndexScanNode annotation) throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getTableId()),
        "Error: There is no table matched to %s", annotation.getTableId());

    Fragment[] fragments = ctx.getTables(annotation.getTableId());

    String indexName = IndexUtil.getIndexNameOfFrag(fragments[0],
        annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableId()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(sm, annotation, fragments[0], new Path(
        indexPath, indexName), annotation.getKeySchema(), comp,
        annotation.getDatum());

  }
}