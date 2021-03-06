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
goog.provide('lf.schema.Column');
goog.provide('lf.schema.Database');
goog.provide('lf.schema.Index');
goog.provide('lf.schema.IndexedColumn');
goog.provide('lf.schema.Table');

goog.forwardDeclare('lf.Order');
goog.forwardDeclare('lf.Predicate');
goog.forwardDeclare('lf.Row');
goog.forwardDeclare('lf.Type');



/**
 * @interface
 */
lf.schema.Column = function() {};


/** @return {string} */
lf.schema.Column.prototype.getName;


/** @return {string} */
lf.schema.Column.prototype.getNormalizedName;


/** @return {!lf.schema.Table} */
lf.schema.Column.prototype.getTable;


/** @return {!lf.Type} */
lf.schema.Column.prototype.getType;


/** @return {?string} */
lf.schema.Column.prototype.getAlias;



/**
 * Models the return value of Database.getSchema().
 * @interface
 */
lf.schema.Database = function() {};


/** @return {string} */
lf.schema.Database.prototype.name;


/** @return {number} */
lf.schema.Database.prototype.version;


/** @return {!Array.<!lf.schema.Table>} */
lf.schema.Database.prototype.tables;


/**
 * @typedef {{
 *   name: string,
 *   order: !lf.Order,
 *   autoIncrement: boolean
 * }}
 */
lf.schema.IndexedColumn;



/**
 * @param {string} tableName
 * @param {string} name
 * @param {boolean} isUnique
 * @param {!Array.<!lf.schema.IndexedColumn>} columns
 * @constructor @struct
 */
lf.schema.Index = function(tableName, name, isUnique, columns) {
  /** @type {string} */
  this.tableName = tableName;

  /** @type {string} */
  this.name = name;

  /** @type {boolean} */
  this.isUnique = isUnique;

  /** @type {!Array.<!lf.schema.IndexedColumn>} */
  this.columns = columns;
};


/** @return {string} */
lf.schema.Index.prototype.getNormalizedName = function() {
  return this.tableName + '.' + this.name;
};



/**
 * Models the return value of Database.getSchema().getTable().
 * @param {string} name
 * @param {!Array.<!lf.schema.Column>} cols
 * @param {!Array.<!lf.schema.Index>} indices
 * @param {boolean} persistentIndex
 *
 * @template UserType, StoredType
 * @constructor
 */
lf.schema.Table = function(name, cols, indices, persistentIndex) {
  /** @private */
  this.name_ = name;

  /** @private {!Array.<!lf.schema.Index>} */
  this.indices_ = indices;

  /** @private {!Array.<!lf.schema.Column>} */
  this.columns_ = cols;

  /** @private {boolean} */
  this.persistentIndex_ = persistentIndex;

  /** @private {?string} */
  this.alias_ = null;
};


/** @return {string} */
lf.schema.Table.prototype.getName = function() {
  return this.name_;
};


/** @return {?string} */
lf.schema.Table.prototype.getAlias = function() {
  return this.alias_;
};


/** @return {string} */
lf.schema.Table.prototype.getEffectiveName = function() {
  return this.alias_ || this.name_;
};


/**
 * @param {string} name
 * @return {!lf.schema.Table}
 */
lf.schema.Table.prototype.as = function(name) {
  // Note1: Can't use lf.schema.Table constructor directly here, because the
  // clone will be missing any auto-generated properties the original object
  // has.

  // Note2: Auto-generated subclasses have a no-arg constructor. The name_
  // parameter is passed to the constructor only for the purposes of
  // lf.testing.MockSchema tables which don't have a no-arg constructor and
  // otherwise would not work with as() in tests.
  var clone = new this.constructor(this.name_);
  clone.alias_ = name;
  return clone;
};


/**
 * @param {UserType=} opt_value
 * @return {!lf.Row.<UserType, StoredType>}
 * @throws {lf.Exception}
 */
lf.schema.Table.prototype.createRow = goog.abstractMethod;


/**
 * @param {{id: number, value: *}} dbRecord
 * @return {!lf.Row.<UserType, StoredType>}
 */
lf.schema.Table.prototype.deserializeRow = goog.abstractMethod;


/** @return {!Array.<!lf.schema.Index>} */
lf.schema.Table.prototype.getIndices = function() {
  return this.indices_;
};


/** @return {!Array.<!lf.schema.Column>} */
lf.schema.Table.prototype.getColumns = function() {
  return this.columns_;
};


/** @return {!lf.schema.Constraint} */
lf.schema.Table.prototype.getConstraint = goog.abstractMethod;


/** @return {boolean} */
lf.schema.Table.prototype.persistentIndex = function() {
  return this.persistentIndex_;
};


/**
 * Row id index is named <tableName>.#, which is an invalid name for JS vars
 * and therefore user-defined indices can never collide with it.
 * @const {string}
 */
lf.schema.Table.ROW_ID_INDEX_PATTERN = '#';


/** @return {string} */
lf.schema.Table.prototype.getRowIdIndexName = function() {
  return this.name_ + '.' + lf.schema.Table.ROW_ID_INDEX_PATTERN;
};
