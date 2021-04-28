import pandas as pd
import DataDocuments
import Connection as con
file_path_links = "../data/links.csv"
file_path_movies = "../data/movies.csv"
file_path_ratings = "../data/ratings.csv"
file_path_tags = "../data/tags.csv"

movies_data = pd.read_csv(file_path_movies)
links_data = pd.read_csv(file_path_links)
ratings_data = pd.read_csv(file_path_ratings)
tags_data = pd.read_csv(file_path_tags)
print(type(movies_data))
movie_coll = con.openCollection("movie_lens","movies")
rating_coll = con.openCollection("movie_lens","user_ratings")
for index in movies_data.index:
    movie = DataDocuments.MOVIE_SAMPLE
    movieid= movies_data["movieId"][index]
    title= movies_data["title"][index]
    genres = movies_data["genres"][index].split("|")
    movie["_id"]=movieid
    movie["title"]=title
    movie["genres"]=genres
    for index2 in ratings_data[ratings_data["movieId"]==movieid].index:
        rating = DataDocuments.RATINGS
        usId=ratings_data["userId"][index2]
        rating["_id"]=str(str(movieid)+"_"+str(usId))
        rating["user_id"] = usId
        rating["movie_id"]=movieid
        rating["rating"] = ratings_data["rating"][index2]
        rating["time_stamp"]= ratings_data["timestamp"][index2]
        tags = []
        for index3 in tags_data[(tags_data["userId"]==usId) & (tags_data["movieId"]==movieid)].index:
            tags.append(tags_data["tag"][index3])
        rating["tags"] = tags
        rating_coll.insert_one(con.correct_encoding(rating))

    for index4 in links_data[links_data["movieId"]==movieid].index:

        movie["imdbid"]=links_data["imdbId"][index4]
        movie["tmdbid"]=links_data["tmdbId"][index4]
    movie_coll.insert_one(con.correct_encoding(movie))















