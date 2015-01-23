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
    ['$scope', 'DbService', 'ResultsService', function(
        $scope, dbService, resultsService) {
      this.clear = function() {
        console.log('clearing...');
        $scope.citySelection = 'unbound';
        $scope.disciplineSelection = 'unbound';
        $scope.countrySelection = 'unbound';
        $scope.genderSelection = 'unbound';
        $scope.colorSelection = 'unbound';
        $scope.fromYearSelection = 'unbound';
        $scope.toYearSelection = 'unbound';
      };

      // TODO(dpapad): Get a list of all years.
      this.fromYears = ['unbound', 2004, 2008, 2012];
      this.toYears = ['unbound', 2004, 2008, 2012];
      // TODO(dpapad): Get a list of all cities.
      this.cities = ['unbound', 'Athens', 'Rome'];
      // TODO(dpapad): Get a list of all disciplines.
      this.disciplines = ['unbound', 'Swimming', 'Athletics'];
      // TODO(dpapad): Get a list of all countries.
      this.countries = ['unbound', 'USA', 'GRE'];
      this.genders = ['unbound', 'Men', 'Women'];
      this.colors = ['unbound', 'Gold', 'Silver', 'Bronze'];
      this.clear();

      this.search = function() {
        this.getQuery_().then(function(query) {
          console.log('executing:', query.toSql());
          return query.exec();
        }).then(function(results) {
          console.log('res', results);
          resultsService.set(results);
        });
      };

      this.getPredicates_ = function() {
        var medal = olympia.db.getSchema().getMedal();
        var predicates = [];

        if ($scope.countrySelection != 'unbound') {
          predicates.push(medal.country.eq($scope.countrySelection));
        }

        if ($scope.colorSelection != 'unbound') {
          predicates.push(medal.color.eq($scope.colorSelection));
        }

        if ($scope.citySelection != 'unbound') {
          predicates.push(medal.city.eq($scope.citySelection));
        }

        if ($scope.genderSelection != 'unbound') {
          predicates.push(medal.gender.eq($scope.genderSelection));
        }

        if ($scope.disciplineSelection != 'unbound') {
          predicates.push(medal.discipline.eq($scope.disciplineSelection));
        }

        if ($scope.fromYearSelection != 'unbound' &&
            $scope.toYearSelection != 'unbound') {
          predicates.push(medal.year.between(
              $scope.fromYearSelection, $scope.toYearSelection));
        } else if ($scope.fromYearSelection != 'unbound') {
          predicates.push(medal.year.gte($scope.fromYearSelection));
        } else if ($scope.toYearSelection != 'unbound') {
          predicates.push(medal.year.lte($scope.toYearSelection));
        }

        return predicates.length > 0 ?
            lf.op.and.apply(null, predicates) :
            null;
      };


      this.getQuery_ = function() {
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
