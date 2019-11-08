var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://192.168.0.145/";

var genresObject = [
	{_id: 1, genre_title: 'Action'},
	{_id: 2, genre_title: 'Comedy'},
	{_id: 3, genre_title: 'Drama'},
];
var reviewersObject = [
	{_id: 1, reviewer_name: 'Peter Jackson'},
	{_id: 3, reviewer_name: 'John Tomston'},
	{_id: 5, reviewer_name: 'Jeff Jefferson'},
];
var moviesObject = [
	{ _id: 1, movie_title: 'Vienas', movie_year: 1999, movie_genres: [{genre_id: 2},{genre_id:1}], rating: [{reviewer_id: 1, rating_stars: 5}, {reviewer_id: 3, rating_stars:4}]},
	{ _id: 2, movie_title: 'Du', movie_year: 2003, movie_genres: [{genre_id: 1}], rating: [{reviewer_id: 1, rating_stars: 4}, {reviewer_id: 3, rating_stars:5}]},
	{ _id: 3, movie_title: 'Trys', movie_year: 2005, movie_genres: [{genre_id: 1},{genre_id:2}], rating: [{reviewer_id: 1, rating_stars: 2}, {reviewer_id: 3, rating_stars:3}]},
	{ _id: 4, movie_title: 'Keturi', movie_year: 2010, movie_genres: [{genre_id: 1},{genre_id:3}], rating: [{reviewer_id: 5, rating_stars: 4}]},

]; 
var resultArray = [];
//createCollections();
//insertData();
//queryEmbedded();
//queryAggregation();
queryMapReduce();
function createCollections(){
		MongoClient.connect(url, function(err, db) {
		  if (err) throw err;
		  var dbo = db.db("mydb");
		  dbo.createCollection("movies", function(err, res) {
		    if (err) throw err;
		    console.log("Movies collection created!");

		    db.close();
		  });
		  dbo.createCollection("reviewers", function(err, res) {
		    if (err) throw err;
		    console.log("Rating collection created!");
		    db.close();

		  });
		  dbo.createCollection("genres", function(err, res) {
		    if (err) throw err;
		    console.log("Genres collection created!");
		    db.close();

		  });
		});
}
function insertData(){
	MongoClient.connect(url, function(err, db) {
		if (err) throw err;
		var dbo = db.db("mydb");
	  dbo.collection('reviewers').insertMany(reviewersObject, function(err, res) {
	  	if (err) throw err;
	  	console.log("inserter: " + res.insertedCount);
	    db.close();

	  });
	    dbo.collection('genres').insertMany(genresObject, function(err, res) {
	  	if (err) throw err;
	  	console.log("inserter: " + res.insertedCount);
	    db.close();

	  });
	    dbo.collection('movies').insertMany(moviesObject, function(err, res) {
	  	if (err) throw err;
	  	console.log("inserter: " + res.insertedCount);
	    db.close();
	  });
	});
}

function queryEmbedded(){

	MongoClient.connect(url, function(err, db) {
		var dbo = db.db("mydb");
		dbo.collection("movies").find({}, { projection: {rating: 1}}).toArray(function(err, result){
			if (err) throw err;

			console.log(JSON.stringify(result, null, 2));
			db.close;
		});
		});
}
function queryAggregation(){
	MongoClient.connect(url, function(err, db) {
		var dbo = db.db("mydb");
		dbo.collection('movies').aggregate([
			{$unwind: "$rating"},
			{$project : {_id : 1, movie_title : 1, movie_year : 1, rating: 1 }},
		    { $lookup:
		       {
		         from: 'reviewers',
		         localField: 'rating.reviewer_id',
		         foreignField: '_id',
		         as: 'reviewee',
		       }
		     },
		    ]).toArray(function(err, res) {
		    if (err) throw err;
		    console.log(JSON.stringify(res, null, 2));
		    db.close();
		});
		});
}
var map = function(){

	var value = new Array();
		var value = {
		movie_id: this._id,
		movie_title: this.movie_title,
		year: this.movie_year,
		 };
		for(var i in this.rating)
				emit({reviewer_id:this.rating[i].reviewer_id}, {movie_id:this._id, movie_title:this.movie_title, year:this.movie_year, rating: this.rating[i].rating_stars} );
}
var mapReviewers = function(){

		emit({reviewer_id:this._id}, {"name" : this.reviewer_name } );
}
var reduce = function(key, value) {
			var results = {};
			var movies = [];
		
	
		//result.reviewer_name
		
		 value.forEach(function(v) {
		 	var movie = {};
		 	if(v.movie_id != undefined ) movie["movie_id"]= v.movie_id;
			if(v.movie_title !== undefined ) movie["movie_title"]= v.movie_title;
			if(v.year !== undefined) movie["year"] = v.year;
			if(v.rating !== undefined) movie["rating_stars"] = v.rating;
			if(Object.keys(movie).length > 0) movies.push(movie);
			if(v.name !== undefined) results["reviewer_name"] = v.name;
			if(v.movies !== undefined) results["movies"] = v.movies;
			//if(Object.keys(movie).length > 0) results["movies"] = movies;
		 });

		if(Object.keys(movies).length > 0) results["movies"] = movies;


		return results;
	}
function queryMapReduce() {
	MongoClient.connect(url, function(err, db) {
		var dbo = db.db("mydb");
		
		dbo.collection('movies').mapReduce(map,	reduce, { out: { reduce: "joined"} });
		dbo.collection("reviewers").mapReduce(mapReviewers, reduce, { out: { reduce: "joined"} });

			db.close();

	});
}