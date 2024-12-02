import json
from functools import reduce
from typing import List

import toolz as t
from toolz.curried import valmap, partial


#
# def reduce_phase(mapped_data):
#     data = t.pipe(
#         t.groupby("rank", mapped_data),
#         t.partial(t.valmap, lambda stats: {'movie_ids': [s['movie_id'] for s in stats],
#             'movies_count': len([s['movie_id'] for s in stats])
#         }))
#     print(json.dumps(data, indent=3))
#
# def reduce_phase(mapped_data):
#     return t.pipe(
#         t.groupby("rank", mapped_data),
#         t.partial(t.valmap, lambda stats: {
#             'movie_ids': [s["movie_id"] for s in stats],
#             'movies_count': len([s["movie_id"] for s in stats])
#         }),
#         lambda data: [
#                 f"{','.join(t.map(str, values['movie_ids']))}\t" +
#                 f"{values['movies_count']}"
#             for values in data.values()
#         ]
#     )
#
# def map_reduce(data: List[str]):
#     return t.pipe(
#         [map_phase(line) for line in data],
#         reduce_phase
#     )
def map_phase(data):
    movie_id, user_id, renk, timestamp = map(int, data.split("\t"))
    return { "movie_id": movie_id, "rank": renk, 'user_id':user_id, 'timestamp': timestamp}




#
# Count Total Ratings
# Count the total number of ratings in the dataset.
#
def count_total_ratings(data):
    return t.pipe(
        data

    )







# Count Unique Users
# Find the total number of unique users who rated any movie.



def count_unique_users(data):
    return t.pipe(
        t.groupby("user_id", data),
        tuple,
        len
    )



# Count Unique Movies
# Count the number of unique movies in the dataset.

def count_unique_movies(data):
    return t.pipe(
        t.groupby("movie_id", data),
        tuple,
        len
    )




# Filter High Ratings
# Filter and count how many ratings are greater than or equal to 4.



def filter_high_ratings(data):
    return t.pipe(
        data,
        t.partial(map, lambda a: a['rank']),
        t.partial(filter, lambda m: m >= 4),
        sum
    )







# Filter Specific User's Ratings
# Extract all ratings made by a specific user ID (e.g., user ID 196).
#



def filter_specific_user_ratings(data, user_id):
    return t.pipe(
        data,
        t.partial(filter, lambda m: m['user_id'] == user_id),
        list
    )


def p(a):
    print(a)
def helper(l):
    return t.pipe(
        l,
        t.partial(t.valmap, lambda x: [s['rank'] for s in x]),

    )
#lambda stats: {'movie_ids': [s['movie_id'] for s in stats]

# Intermediate Exercises
# Average Rating per Movie
# Calculate the average rating for each movie.
#lambda stats: {'movie_ids': [s['movie_id'] for s in stats]
def average_rating_per_movie(data):
   return t.pipe(
        t.groupby("movie_id", data),
        helper,
        t.partial(t.valmap, lambda x: {
            "average_rating": sum(x) / len(x),
            "num_ratings": len(x)
        })
   )


# Top 5 Movies by Average Rating
# Find the top 5 movies with the highest average ratings (movies with the same average rating can be ordered by the number of ratings).

def top_5_movies(data):
    avg_rating_with_count = average_rating_per_movie(data)

    # Sort movies by average rating (desc) and number of ratings (desc)
    sorted_movies = sorted(
        avg_rating_with_count.items(),
        key=lambda item: (item[1]["average_rating"], item[1]["num_ratings"]),
        reverse=True
    )
    return sorted_movies[:5]
with open("../data/u.data", "r") as file:
    x = [ map_phase(line) for line in file.readlines()][:1000]
    print(top_5_movies(x))


# Bottom 5 Movies by Average Rating
# Identify the 5 movies with the lowest average ratings.









# Ratings Per User
# Calculate the total number of ratings given by each user.
# Movies Rated by User
# Create a list of movie IDs rated by each user.
# Most Active Users
# Identify the 5 users who rated the most movies.
# Least Active Users
# Identify the 5 users who rated the fewest movies.
#
# Advanced Exercises
# Top N Movies Rated by User Count
# Find the movies that have the highest number of ratings.
# Movies Rated Exactly Once
# Identify movies that have been rated by only one user.
# Highest Rated Movie by Each User
# Determine the highest-rated movie for each user.
# Rating Distribution per Movie
# Create a distribution of ratings (e.g., how many 1, 2, 3, 4, 5 ratings) for each movie.
# Find Movies Rated by more than one user
# Identify movies that have been rated by more than one user
# Find Users Who Rated the Same Movie
# Group users who rated the same movie.
# Ratings in a Time Range
# Extract all ratings that occurred within a specific timestamp range (e.g., 881250949 to 891717742).
# Most Rated Movie in a Time Range
# Find the movie with the highest number of ratings in a specific time range.
# Users Rating Movies on the Same Day
# Identify users who rated movies on the same timestamp.
#
# Exploration and Aggregation
#  22. Average Ratings Over Time
# Calculate the average rating for all movies over specific time intervals.
# Top Movies by Total Rating Sum
# Find movies with the highest total sum of ratings (e.g., movies with many high ratings).
# Users Who Gave All High Ratings
# Identify users who rated every movie with a score of 5.
# Users Who Gave All Low Ratings
# Find users who rated every movie with a score of 1.
# Find Users Who Rated the Same Movie with Different Ratings
# Identify users who rated the same movie differently on different occasions.
#
# Advanced Challenges
#   27. Filter Ratings by Movie Genre (Simulated)
# If each movie ID represents a genre, calculate the average rating per genre.
#   28. Normalize Ratings
# Normalize ratings for each movie such that the highest rating for each movie is scaled to 1.

