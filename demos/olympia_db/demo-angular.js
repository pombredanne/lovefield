var app = angular.module('myApp', []);

app.service('DbService', function($http) {
  var db = null;
  var initialized = false;

  /**
   * Ensures that database is populated with data and initialized the DB
   * connection.
   * @private
   */
  this.init_ = function() {
    return olympia.db.getInstance().then((function(database) {
      db = database;
      return this.checkForExistingData_();
    }).bind(this)).then((function(dataExist) {
      return dataExist ? Promise.resolve() : this.insertData_();
    }).bind(this));
  }

  /**
   * Gets the db connection.
   */
  this.get = function () {
    if (initialized) {
      return Promise.resolve(db);
    }

    return promise.then(function() {
      return db;
    });
  };


  /**
   * Checks if any data exists already in the DB.
   * @private
   */
  this.checkForExistingData_ = function() {
    var medal = db.getSchema().getMedal();
    return db.select().from(medal).exec().then(
        function(rows) {
          return rows.length > 0;
        });
  };


  /**
   * Inserts data to the DB.
   * @private
   */
  this.insertData_ = function() {
    var medal = db.getSchema().getMedal();
    return $http.get('data/olympic_medalists.json').then(
        function(response) {
          var rows = response.data.map(function(obj) {
            return medal.createRow(obj);
          });
          return db.insert().into(medal).values(rows).exec();
        });
  };

  // Trigger DB initialization.
  this.init_().then(function() {
    initialized = true;
    console.log('DB connection ready.');
  });
});


app.service('ResultsService', function() {
  this.results = [];

  this.get = function() {
    return this.results;
  };

  this.set = function(results) {
    this.results = results;
  };
});


app.controller(
    'ResultsController',
    ['$scope', 'ResultsService', function($scope, resultsService) {
      this.getResults = function() {
        return resultsService.get();
      }
    }]);


app.controller(
    'QueryBuilderController',
    ['$scope', '$http', 'DbService', 'ResultsService', function(
        $scope, $http, dbService, resultsService) {

      var unboundValue = undefined;

      this.clear = function() {
        // Removing all predicates.
        $scope.citySelection = unboundValue;
        $scope.disciplineSelection = unboundValue;
        $scope.countrySelection = unboundValue;
        $scope.genderSelection = unboundValue;
        $scope.colorSelection = unboundValue;
        $scope.fromYearSelection = unboundValue;
        $scope.toYearSelection = unboundValue;

        // Removing last results, if any.
        resultsService.set([]);

        // Clearing SQL query.
        $scope.sqlQuery = '';
      };

      this.populateUi_ = function() {
         return $http.get('data/column_domains.json').then(
             (function(response) {
               var domains = response.data;
               this.fromYears = domains.years;
               this.toYears = domains.years;
               this.cities = domains.cities;
               this.disciplines = domains.disciplines;
               this.countries = domains.countries;
               this.genders = domains.genders;
               this.colors = domains.colors;
             }).bind(this));
      };

      this.fromYears = [];
      this.toYears = [];
      this.cities = [];
      this.disciplines = [];
      this.countries = [];
      this.genders = [];
      this.colors = [];
      this.sqlQuery = '';
      this.populateUi_();

      this.search = function() {
        this.buildQuery_().then(function(query) {
          $scope.sqlQuery = query.toSql();
          console.log('executing:', query.toSql());
          return query.exec();
        }).then(function(results) {
          resultsService.set(results);
        });
      };

      this.getPredicates_ = function() {
        var medal = olympia.db.getSchema().getMedal();
        var predicates = [];

        if ($scope.countrySelection != unboundValue) {
          predicates.push(medal.country.eq($scope.countrySelection));
        }

        if ($scope.colorSelection != unboundValue) {
          predicates.push(medal.color.eq($scope.colorSelection));
        }

        if ($scope.citySelection != unboundValue) {
          predicates.push(medal.city.eq($scope.citySelection));
        }

        if ($scope.genderSelection != unboundValue) {
          predicates.push(medal.gender.eq($scope.genderSelection));
        }

        if ($scope.disciplineSelection != unboundValue) {
          predicates.push(medal.discipline.eq($scope.disciplineSelection));
        }

        if ($scope.fromYearSelection != unboundValue &&
            $scope.toYearSelection != unboundValue) {
          var minYear = Math.min(
              $scope.fromYearSelection, $scope.toYearSelection);
          var maxYear = Math.max(
              $scope.fromYearSelection, $scope.toYearSelection);
          predicates.push(medal.year.between(minYear, maxYear));
        } else if ($scope.fromYearSelection != unboundValue) {
          predicates.push(medal.year.gte($scope.fromYearSelection));
        } else if ($scope.toYearSelection != unboundValue) {
          predicates.push(medal.year.lte($scope.toYearSelection));
        }

        return predicates.length > 0 ?
            lf.op.and.apply(null, predicates) :
            null;
      };


      this.buildQuery_ = function() {
        return dbService.get().then((function(db) {
          var predicates = this.getPredicates_();
          var medal = olympia.db.getSchema().getMedal();
          var query = predicates != null ?
              db.select().from(medal).where(predicates) :
              db.select().from(medal);
          return query;
        }).bind(this));
      };

    }]);
