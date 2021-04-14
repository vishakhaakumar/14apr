#ifndef MOVIES_MICROSERVICES_RECOMMENDERHANDLER_H
#define MOVIES_MICROSERVICES_RECOMMENDERHANDLER_H

#include <iostream>
#include <string>
#include <regex>
#include <future>
#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/RecommenderService.h"
#include "../../gen-cpp/MovieInfoService.h"
#include "../../gen-cpp/UserLikesService.h"

#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"

namespace movies{

class RecommenderServiceHandler : public RecommenderServiceIf {
 public:
  RecommenderServiceHandler(
          mongoc_client_pool_t *,
		  ClientPool<ThriftClient<MovieInfoServiceClient>> *,
		  ClientPool<ThriftClient<UserLikesServiceClient>> *) ;
  ~RecommenderServiceHandler() override=default;

  void GetRecommendations(std::vector<std::string>& _return, const int64_t user) override;
  void UploadRecommendations(const int64_t user_id, const std::vector<std::string> & movie_id) override;
 private:
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<MovieInfoServiceClient>> *_movie_info_client_pool;
  ClientPool<ThriftClient<UserLikesServiceClient>> *_user_likes_client_pool;
};

// Constructor
RecommenderServiceHandler::RecommenderServiceHandler(
        mongoc_client_pool_t *mongodb_client_pool,
		ClientPool<ThriftClient<MovieInfoServiceClient>> *movie_info_client_pool,
		ClientPool<ThriftClient<UserLikesServiceClient>> *user_likes_client_pool) {

     // Storing the clientpool
     _mongodb_client_pool = mongodb_client_pool;
     _movie_info_client_pool = movie_info_client_pool;
	 _user_likes_client_pool = user_likes_client_pool;
}

// Remote Procedure "UploadRecommendations"
void RecommenderServiceHandler::UploadRecommendations(const int64_t user_id, const std::vector<std::string> & movie_id){

    // Get mongo client
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(_mongodb_client_pool);

    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }

    // Get mongo collection
    auto collection = mongoc_client_get_collection(
        mongodb_client, "recommender", "recommender");

    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB recommender";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    // Check if the recommendations for this user already exist in the database
    bson_t *query = bson_new();
    BSON_APPEND_INT64(query, "user_id", user_id);

    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);

   if (found) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "User ID " + std::to_string(user_id) + " already existed in MongoDB Recommender";
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
   } else {
      bson_t *new_doc = bson_new();
      BSON_APPEND_INT64(new_doc, "user_id", user_id);

      const char *key;
      int idx = 0;
      char buf[16];
      bson_t movie_id_list;
      BSON_APPEND_ARRAY_BEGIN(new_doc, "movie_ids", &movie_id_list);
      for (auto &mid : movie_id) {
         bson_uint32_to_string(idx, &key, buf, sizeof buf);
         BSON_APPEND_UTF8(&movie_id_list, key, mid.c_str());
         idx++;
      }
      bson_append_array_end(new_doc, &movie_id_list);

      bson_error_t error;
      bool plotinsert = mongoc_collection_insert_one (collection, new_doc, nullptr, nullptr, &error);

      if (!plotinsert) {
         LOG(error) << "Failed to insert recommendations for " << std::to_string(user_id) << " to MongoDB: " << error.message;
         ServiceException se;
         se.errorCode = ErrorCode::SE_MONGODB_ERROR;
         se.message = error.message;
         bson_destroy(new_doc);
         mongoc_cursor_destroy(cursor);
         mongoc_collection_destroy(collection);
         mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
         throw se;
      }
      bson_destroy(new_doc);
   }

    // Cleanup mongo
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    mongoc_cleanup ();
}

// Remote Procedure "GetRecommendations"
void RecommenderServiceHandler::GetRecommendations(std::vector<std::string>& _return, const int64_t user){

    // Initialize empty movie_id list
    std::vector<std::string> movie_ids;

    // Get mongo client
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(_mongodb_client_pool);

    // Get mongo collection
    auto collection = mongoc_client_get_collection(
        mongodb_client, "recommender", "recommender");

    if (!collection) {
       ServiceException se;
       se.errorCode = ErrorCode::SE_MONGODB_ERROR;
       se.message = "Failed to create collection user from DB recommender";
       mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
       throw se;
    }

    // Get movie recommendations for this user from database
    bson_t *query = bson_new();
    BSON_APPEND_INT64(query, "user_id", user);

    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);

    if (found) {
       auto recommendations_json_char = bson_as_json(doc, nullptr);
       json recommendations_json = json::parse(recommendations_json_char);

       for (auto &movie_id : recommendations_json["movie_ids"]) {
          movie_ids.push_back(movie_id);
       }
    }

     movie_ids.push_back("MOV0001");
     movie_ids.push_back("MOV0003");

    if (movie_ids.size() > 0) {

        // Get the movie info service client pool
        auto movie_info_client_wrapper = _movie_info_client_pool->Pop();
        if (!movie_info_client_wrapper) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
            se.message = "Failed to connect to movie-info-service";
            throw se;
        }
        auto movie_info_client = movie_info_client_wrapper->GetClient();

        // Call the remote procedure : GetMoviesByIds
        std::vector<std::string> _return_movies;
        try {
           // movie_info_client->GetMoviesByIds(_return_movies, movie_ids);
           // _return = _return_movies;
	   //
	    movie_info_client->GetMoviesByIds(_return_movies, movie_ids);
	    _return = _return_movies;
        } catch (...) {
            _movie_info_client_pool->Push(movie_info_client_wrapper);
            LOG(error) << "Failed to send call GetMoviesByIds to movie-info-client";
            throw;
        }
        _movie_info_client_pool->Push(movie_info_client_wrapper);
    }

    // Cleanup mongo
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    mongoc_cleanup ();
}

} // namespace movies


#endif //MOVIES_MICROSERVICES_RECOMMENDERHANDLER_H

