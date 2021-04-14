#ifndef MOVIES_MICROSERVICES_MOVIEINFOHANDLER_H
#define MOVIES_MICROSERVICES_MOVIEINFOHANDLER_H

#include <iostream>
#include <string>
#include <regex>
#include <future>
#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/MovieInfoService.h"

#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"

namespace movies{

class MovieInfoServiceHandler : public MovieInfoServiceIf {
 public:
  MovieInfoServiceHandler(mongoc_client_pool_t *,mongoc_client_t *);
  ~MovieInfoServiceHandler() override=default;

  void GetMoviesByIds(std::vector<std::string>& _return, const std::vector<std::string> & movie_ids) override;
  void GetMoviesByTitle(std::vector<std::string> & _return, const std::string& movie_string) override;
 private:
    mongoc_client_pool_t *_mongodb_client_pool;
    mongoc_client_t *mongodb_client;
};
    // Constructor
MovieInfoServiceHandler::MovieInfoServiceHandler(mongoc_client_pool_t *mongodb_client_pool, mongoc_client_t *_mongodb_client) {
 // Storing the clientpool
       _mongodb_client_pool = mongodb_client_pool;
       mongodb_client = _mongodb_client;
}
	
 void MovieInfoServiceHandler::GetMoviesByTitle(std::vector<std::string> & _return, const std::string& movie_string){
	 std::cout << "************** Inside get movies by title *************** !!!!!!!! ..." << movie_string <<" end "<< std::endl;
	auto collection = mongoc_client_get_collection(
		         mongodb_client, "movies", "movie-info");

          if (!collection) {
           ServiceException se;
           se.errorCode = ErrorCode::SE_MONGODB_ERROR;
           se.message = "Failed to create collection user from DB recommender";
            mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
           throw se;
                  }

          bson_t *query = bson_new();
	  const std::string& i = "i"; 
     	  bson_append_regex(query,"movie_title", -1, movie_string.c_str(),i.c_str());
	  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
	  
       	  const bson_t *doc;
	  while (mongoc_cursor_next (cursor, &doc)) {

            auto movietitle_json_char = bson_as_json(doc, nullptr);
	    json movietitle_json = json::parse(movietitle_json_char);
	      for (auto &titlev : movietitle_json["movie_title"]) {
	     std::cout << "movie found is ------>>>>  !!!!!!! ..."<< titlev <<"  " << std::endl;
	    _return.push_back(titlev);
	       }
	      doc = NULL;
	  }
	    mongoc_cursor_destroy(cursor);
	   mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);	
}

// Remote Procedure "GetMoviesById"

void MovieInfoServiceHandler::GetMoviesByIds(std::vector<std::string>& _return, const std::vector<std::string> & movie_ids) {
    // This is a placeholder, used to confirm that RecommenderService can communicate properly with MovieInfoService
    // Once the MongoDB is up and running, this should return movie titles that match the given ids.
	std::cout << "called hereeeee  here !!!!!!!! ..." << std::endl; 
	  
	auto collection = mongoc_client_get_collection(
         mongodb_client, "movies", "movie-info");

	  if (!collection) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to create collection user from DB recommender";
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
		 }

  
          bson_t *query = bson_new();
         // BSON_APPEND_INT64 (query, "movie_id",789);
	  BSON_APPEND_UTF8 (query, "title","Spiderman I");

          mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
          const bson_t *doc;
          bool found = mongoc_cursor_next(cursor, &doc);

	  if (found) {
	      auto movietitle_json_char = bson_as_json(doc, nullptr);
	      json movietitle_json = json::parse(movietitle_json_char);
	       for (auto &titlev : movietitle_json["movie_url"]) {
		       std::cout << "movie found is ------>>>>  !!!!!!! ..."<< titlev <<"  " << std::endl;
	                 _return.push_back(titlev);
		        }
	       mongoc_cursor_destroy(cursor);
	      //  mongoc_collection_destroy(collection);
	        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
	     }else {
		 for (auto &movie_id : movie_ids) {
		 _return.push_back("MovieInfoService will provide title for movie with id: " + movie_id);
		 }
	     }
    }

} // namespace movies


#endif //MOVIES_MICROSERVICES_MOVIEINFOHANDLER_H

