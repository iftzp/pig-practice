register file:/home/ivan/Documents/ca4022/pig-0.17.0/lib/piggybank.jar

movie_data = load 'ml-latest-small/movies.csv' using PigStorage(',') as (movieId:int, title:chararray, genres:chararray);

rating_data = load 'ml-latest-small/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:double, timestamp:int);

--prepare rating data
grouped_movie_rating = group rating_data by (movieId, rating); --each movie and rating ordered into movieId groups
movie_ratings_counts = foreach grouped_movie_rating generate group.movieId, group.rating, COUNT(rating_data) as rating_count; --give each rating a count of occurences for each movie

--get movie with most ratings
movie_ratings_grouped = group movie_ratings_counts by movieId; --movie then rating, count

movie_total_ratings = foreach movie_ratings_grouped generate group as movieId, SUM(movie_ratings_counts.rating_count) as total_rating_count; --movie and total number of times it has been rated

movie_total_ratings = order movie_total_ratings by total_rating_count desc; --sort descending

most_ratings = limit movie_total_ratings 1; --this is the movie with most ratings

movie_with_most_ratings = join movie_data by movieId, most_ratings by movieId; --link this back to the movie data
--dump movie_with_most_ratings; --shows most rated movie in terminal

--find movie with highest number of 5* ratings
five_star_ratings = filter movie_ratings_counts by (rating == 5); --apply a filter to only show 5 star ratings
five_star_ratings = order five_star_ratings by rating_count desc; --sort descending
most_five_star_ratings = limit five_star_ratings 1; --this is the movie with most 5 star ratings

movie_with_most_five_star_ratings = join movie_data by movieId, most_five_star_ratings by movieId; --link to movie data - this should be ok as we have limited to 1
--dump movie_with_most_five_star_ratings; --show movie with most 5 star ratings in terminal


--find user with highest average rating

grouped_user_rating = group rating_data by (userId, rating); --each user and rating ordered into userId groups
user_rating_counts = foreach grouped_user_rating generate group.userId, group.rating, COUNT(rating_data) as rating_count; --give each rating a count of occurences for each user

user_ratings_grouped = group user_rating_counts by userId; --user then rating, count

user_avg_rating = foreach user_ratings_grouped {
    total_rating = foreach user_rating_counts generate rating * rating_count;
    generate group as userId, SUM(total_rating) / SUM(user_rating_counts.rating_count) as avg_rating;
}; --we need to get each user, then for each rating multiply by number of times it occurs. then divide by total number of ratings given by that user

user_avg_rating = order user_avg_rating by avg_rating desc; --sort descending
highest_avg = limit user_avg_rating 1; --this is the user who has given the highest average rating

--dump highest_avg; --show user with highest avg rating
