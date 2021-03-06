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
goog.setTestOnly();
goog.require('goog.functions');
goog.require('goog.structs.Set');
goog.require('goog.testing.AsyncTestCase');
goog.require('goog.testing.PropertyReplacer');
goog.require('goog.testing.jsunit');
goog.require('goog.userAgent.product');
goog.require('hr.bdb');
goog.require('hr.db');
goog.require('lf.Row');
goog.require('lf.backstore.BundledObjectStore');
goog.require('lf.backstore.IndexedDB');
goog.require('lf.backstore.Memory');
goog.require('lf.backstore.ObjectStore');
goog.require('lf.backstore.TableType');
goog.require('lf.cache.DefaultCache');
goog.require('lf.index.IndexMetadata');
goog.require('lf.index.IndexMetadataRow');
goog.require('lf.service');


/** @type {!goog.testing.AsyncTestCase} */
var asyncTestCase = goog.testing.AsyncTestCase.createAndInstall('IndexedDB');


/** @type {!goog.testing.PropertyReplacer} */
var propertyReplacer;


function setUp() {
  propertyReplacer = new goog.testing.PropertyReplacer();
}


function tearDown() {
  propertyReplacer.reset();
}


/**
 * Randomizes the schema name such that a new DB is created for every test run.
 * @param {!lf.schema.Database} schema
 * @return {!lf.schema.Database} A schema to be used for testing.
 */
function randomizeSchemaName(schema) {
  propertyReplacer.replace(schema, 'name', function() {
    return 'db_' + goog.string.getRandomString();
  });

  return schema;
}


function testInit_IndexedDB_NonBundled() {
  var schema = randomizeSchemaName(hr.db.getSchema());
  var global = hr.db.getGlobal();
  checkInit_IndexedDB(schema, global, false);
}


function testInit_IndexedDB_Bundled() {
  var schema = randomizeSchemaName(hr.bdb.getSchema());
  var global = hr.bdb.getGlobal();
  var cache = new lf.cache.DefaultCache();
  global.registerService(lf.service.CACHE, cache);

  checkInit_IndexedDB(schema, global, true);
}


/**
 * Initializes a DB with the given schema and performs assertions on the tables
 * that were created.
 *
 * @param {!lf.schema.Database} schema
 * @param {!lf.Global} global
 * @param {boolean} bundledMode Whether to initialize the DB in BUNDLED mode.
 */
function checkInit_IndexedDB(schema, global, bundledMode)  {
  if (goog.userAgent.product.SAFARI) {
    return;
  }

  var description = 'testInit_IndexedDB_' +
      bundledMode ? 'Bundled' : 'NonBundled';
  asyncTestCase.waitForAsync(description);

  assertTrue(schema.getHoliday().persistentIndex());

  var indexedDb = new lf.backstore.IndexedDB(global, schema, bundledMode);
  indexedDb.init().then(
      /** @suppress {accessControls} */
      function() {
        var createdTableNames = new goog.structs.Set(
            indexedDb.db_.objectStoreNames);
        assertUserTables(schema, createdTableNames);
        assertIndexTables(schema, createdTableNames);

        return checkAllIndexMetadataExist_IndexedDb(
            indexedDb.db_, schema, global, bundledMode);
      }).then(
      function() {
        asyncTestCase.continueTesting();
      }, fail);
}


function testInit_Memory() {
  asyncTestCase.waitForAsync('testInit_Memory');

  var schema = randomizeSchemaName(hr.db.getSchema());
  assertTrue(schema.getHoliday().persistentIndex());

  var memoryDb = new lf.backstore.Memory(schema);
  memoryDb.init().then(
      /** @suppress {accessControls} */
      function() {
        var createdTableNames = new goog.structs.Set(
            memoryDb.tables_.getKeys());
        assertUserTables(schema, createdTableNames);
        assertIndexTables(schema, createdTableNames);

        return checkAllIndexMetadataExist_MemoryDb(memoryDb, schema);
      }).then(
      function() {
        asyncTestCase.continueTesting();
      }, fail);
}


/**
 * Asserts that an object store was created for each user-defined table.
 * @param {!lf.schema.Database} schema The database schema being tested.
 * @param {!goog.structs.Set<string>} tableNames The names of the tables that
 *     were created in the backing store.
 */
function assertUserTables(schema, tableNames) {
  schema.tables().forEach(function(tableSchema) {
    assertTrue(tableNames.contains(tableSchema.getName()));
  });
}


/**
 * Asserts that an object store was created for each lf.schema.Index instance
 * that belongs to a user-defined table that has "persistentIndex" enabled.
 * @param {!lf.schema.Database} schema The database schema being tested.
 * @param {!goog.structs.Set<string>} tableNames The names of the tables that
 *     were created in the backing store.
 */
function assertIndexTables(schema, tableNames) {
  schema.tables().forEach(function(tableSchema) {
    tableSchema.getIndices().forEach(function(indexSchema) {
      assertEquals(
          tableSchema.persistentIndex(),
          tableNames.contains(indexSchema.getNormalizedName()));
    });

    // Checking whether backing store for RowId index was created.
    assertEquals(
        tableSchema.persistentIndex(),
        tableNames.contains(tableSchema.getRowIdIndexName()));
  });
}


/**
 * Checks that the given backing store is populated with the index metadata.
 * @param {!lf.Stream} objectStore
 * @param {string} indexName
 * @return {!IThenable}
 */
function checkIndexMetadataExist(objectStore, indexName) {
  var expectedIndexType = indexName.substr(-1) == '#' ?
      lf.index.IndexMetadata.Type.ROW_ID :
      lf.index.IndexMetadata.Type.BTREE;

  return objectStore.get([]).then(function(results) {
    assertEquals(1, results.length);
    var metadataRow = results[0];
    assertEquals(
        expectedIndexType, metadataRow.payload().type);
    assertEquals(
        lf.index.IndexMetadataRow.ROW_ID, metadataRow.id());
  });
}


/**
 * Checks that index metadata have been persisted for all given indices, for the
 * case if an IndexedDB backing store.
 * @param {!IDBDatabase} db
 * @param {!lf.schema.Database} schema
 * @param {!lf.Global} global
 * @param {boolean} bundledMode
 * @return {!IThenable}
 */
function checkAllIndexMetadataExist_IndexedDb(
    db, schema, global, bundledMode) {
  var indexNames = getPersistedIndices(schema);

  var getObjectStore = function(nativeObjectStore) {
    return bundledMode ?
        lf.backstore.BundledObjectStore.forTableType(
            global, nativeObjectStore, lf.Row.deserialize,
            lf.backstore.TableType.INDEX) :
        new lf.backstore.ObjectStore(nativeObjectStore, lf.Row.deserialize);
  };

  var tx = db.transaction(indexNames, 'readonly');
  var promises = indexNames.map(function(indexName) {
    var objectStore = getObjectStore(tx.objectStore(indexName));
    return checkIndexMetadataExist(objectStore, indexName);
  });

  return goog.Promise.all(promises);
}


/**
 * Checks that index metadata have been persisted for all given indices, for the
 * case if an MemoryDb backing store.
 * @param {!lf.backstore.Memory} db
 * @param {!lf.schema.Database} schema
 * @return {!IThenable}
 */
function checkAllIndexMetadataExist_MemoryDb(db, schema) {
  var indexNames = getPersistedIndices(schema);

  var promises = indexNames.map(function(indexName) {
    var objectStore = db.getTableInternal(indexName);
    return checkIndexMetadataExist(objectStore, indexName);
  });

  return goog.Promise.all(promises);
}


/**
 * Finds the names of all indices that are persisted.
 * @param {!lf.schema.Database} schema
 * @return {!Array<string>}
 */
function getPersistedIndices(schema) {
  var indexNames = [];

  schema.tables().forEach(function(tableSchema) {
    if (tableSchema.persistentIndex()) {
      tableSchema.getIndices().forEach(function(indexSchema) {
        indexNames.push(indexSchema.getNormalizedName());
      });

      // Adding RowID index name to the returned array.
      indexNames.push(tableSchema.getRowIdIndexName());
    }
  });

  return indexNames;
}
