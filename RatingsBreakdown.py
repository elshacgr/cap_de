from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings),
                MRStep(reducer=self.reducer_sort_output)]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_output(self, _, count_rating_pairs):
        for count, rating in sorted(count_rating_pairs, reverse=True):
            yield rating, count

#    def mapper_sort_by_rating_counts(self, rating, count):
#        yield str(count).zfill(5), (rating, count)

#    def reducer_output_sorted_ratings(self, count, rating_counts):
#        for rating, count in sorted(rating_counts, reverse=True):
#            yield rating, count


if __name__ == '__main__':
    RatingsBreakdown.run()
