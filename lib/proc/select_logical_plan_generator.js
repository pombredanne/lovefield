/**
 * @license
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
goog.provide('lf.proc.SelectLogicalPlanGenerator');

goog.require('lf.proc.AndPredicatePass');
goog.require('lf.proc.BaseLogicalPlanGenerator');
goog.require('lf.proc.CrossProductNode');
goog.require('lf.proc.CrossProductPass');
goog.require('lf.proc.ImplicitJoinsPass');
goog.require('lf.proc.LimitNode');
goog.require('lf.proc.LogicalPlanRewriter');
goog.require('lf.proc.OrderByNode');
goog.require('lf.proc.ProjectNode');
goog.require('lf.proc.PushDownSelectionsPass');
goog.require('lf.proc.SelectNode');
goog.require('lf.proc.SkipNode');
goog.require('lf.proc.TableAccessNode');



/**
 * @constructor
 * @struct
 * @extends {lf.proc.BaseLogicalPlanGenerator.<!lf.query.SelectContext>}
 *
 * @param {!lf.query.SelectContext} query
 */
lf.proc.SelectLogicalPlanGenerator = function(query) {
  lf.proc.SelectLogicalPlanGenerator.base(this, 'constructor', query);

  /** @private {Array.<!lf.proc.LogicalQueryPlanNode>} */
  this.tableAccessNodes_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.crossProductNode_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.selectNode_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.orderByNode_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.skipNode_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.limitNode_ = null;

  /** @private {lf.proc.LogicalQueryPlanNode} */
  this.projectNode_ = null;
};
goog.inherits(lf.proc.SelectLogicalPlanGenerator,
    lf.proc.BaseLogicalPlanGenerator);


/** @override */
lf.proc.SelectLogicalPlanGenerator.prototype.generateInternal = function() {
  this.generateNodes_();
  this.connectNodes_();

  // Optimizing the "naive" logical plan.
  var rewritePasses = [
    new lf.proc.AndPredicatePass(),
    new lf.proc.CrossProductPass(),
    new lf.proc.PushDownSelectionsPass(),
    new lf.proc.ImplicitJoinsPass()
  ];

  var planRewriter = new lf.proc.LogicalPlanRewriter(
      /** @type {!lf.proc.LogicalQueryPlanNode} */ (this.projectNode_),
      rewritePasses);
  return planRewriter.generate();
};


/**
 * Generates all the nodes that will make up the logical plan tree. After
 * this function returns all nodes have been created, but they are not yet
 * connected to each other.
 * @private
 */
lf.proc.SelectLogicalPlanGenerator.prototype.generateNodes_ = function() {
  this.generateTableAcessNodes_();
  this.generateCrossProductNode_();
  this.generateSelectNode_();
  this.generateOrderByNode_();
  this.generateSkipNode_();
  this.generateLimitNode_();
  this.generateProjectNode_();
};


/**
 * Connects the nodes together such that the logical plan tree is formed.
 * @private
 */
lf.proc.SelectLogicalPlanGenerator.prototype.connectNodes_ = function() {
  var parentOrder = [
    this.projectNode_, this.limitNode_, this.skipNode_, this.orderByNode_,
    this.selectNode_, this.crossProductNode_
  ];

  var lastExistingParentIndex = 0;
  for (var i = 1; i < parentOrder.length; i++) {
    if (!goog.isNull(parentOrder[i])) {
      parentOrder[lastExistingParentIndex].addChild(parentOrder[i]);
      lastExistingParentIndex = i;
    }
  }

  this.tableAccessNodes_.forEach(function(tableAccessNode) {
    parentOrder[lastExistingParentIndex].addChild(tableAccessNode);
  });
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateTableAcessNodes_ =
    function() {
  this.tableAccessNodes_ = this.query.from.map(function(table) {
    return new lf.proc.TableAccessNode(table);
  }, this);
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateCrossProductNode_ =
    function() {
  if (this.query.from.length >= 2) {
    this.crossProductNode_ = new lf.proc.CrossProductNode();
  }
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateSelectNode_ = function() {
  this.selectNode_ = goog.isDefAndNotNull(this.query.where) ?
      new lf.proc.SelectNode(this.query.where.copy()) : null;
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateOrderByNode_ = function() {
  if (this.query.orderBy) {
    this.orderByNode_ = new lf.proc.OrderByNode(this.query.orderBy);
  }
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateLimitNode_ = function() {
  if (this.query.limit) {
    this.limitNode_ = new lf.proc.LimitNode(this.query.limit);
  }
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateSkipNode_ = function() {
  if (this.query.skip) {
    this.skipNode_ = new lf.proc.SkipNode(this.query.skip);
  }
};


/** @private */
lf.proc.SelectLogicalPlanGenerator.prototype.generateProjectNode_ = function() {
  this.projectNode_ = new lf.proc.ProjectNode(
      this.query.columns || [],
      this.query.groupBy || null);
};
