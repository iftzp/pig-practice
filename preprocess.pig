register file:/home/ivan/Documents/ca4022/pig-0.17.0/lib/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

movie_data = load 'ml-latest-small/movies.csv' using CSVLoader(',') as (movieId:int, title:chararray, genres:chararray);

rating_data = load 'ml-latest-small/ratings.csv' using CSVLoader(',') as (userId:int, movieId:int, rating:double, timestamp:int);

movie_data = filter movie_data by movieId is not null;
rating_data = filter rating_data by userId is not null;

movie_data = foreach movie_data generate
	movieId,
	REGEX_EXTRACT(title, '\\((\\d+)\\)', 1) as year,
	REGEX_EXTRACT(title, '([\\S ]+) \\(\\d+\\)', 1) as title,
	genres;

store movie_data into 'output/movie_data' using PigStorage('\t', '--schema');
store rating_data into 'output/rating_data' using PigStorage('\t', '--schema');

fs -getmerge output/movie_data output/movie_data.csv;
fs -getmerge output/rating_data output/rating_data.csv;
