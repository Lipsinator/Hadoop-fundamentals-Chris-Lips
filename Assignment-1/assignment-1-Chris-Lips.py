from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies, combiner=self.combiner_count_ratings, reducer=self.reducer_sum_rating_counts),
            MRStep(reducer=self.reducer_sort_movies_by_ratings)
        ]

    # Get the ids with the mapper from all the movies.
    def mapper_get_movies(self, _, line):
        (_, movieID, _, _) = line.split('\t')
        yield movieID, 1
    
    # Combine the total count of ratings in the id of each movie.
    def combiner_count_ratings(self, movie_id, ratings):
        yield movie_id, sum(ratings)
    
    # Sum the total count of all the ratings.
    def reducer_sum_rating_counts(self, movie_id, ratings):
        yield None, (sum(ratings), movie_id)

    # Sort the movies on their rating.   
    def reducer_sort_movies_by_ratings(self, _, movies):
        for count, movie_id in sorted(movies):
            yield (int(movie_id), int(count))

if __name__ == "__main__":
    Ratings.run()
