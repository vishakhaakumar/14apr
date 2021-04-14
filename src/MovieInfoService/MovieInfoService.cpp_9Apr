#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <signal.h>

#include "../utils.h"
#include "../utils_mongodb.h"
#include "MovieInfoHandler.h"
#include <mongoc.h>
#include <bson/bson.h>
#include <iostream>

using json = nlohmann::json;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;

using namespace movies;


// signal handler code
void sigintHandler(int sig) {
	exit(EXIT_SUCCESS);
}

// entry of this service
int main(int argc, char **argv) {
  // 1: notify the singal handler if interrupted
  signal(SIGINT, sigintHandler);
  // 1.1: Initialize logging
  init_logger();


  // 2: read the config file for ports and addresses
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  // 3: get my port
  int my_port = config_json["movie-info-service"]["port"];

// Get mongodb client pool
   mongoc_client_pool_t* mongodb_client_pool =
   init_mongodb_client_pool(config_json, "movies", 128);
        	 
 	 	 std::cout << "Mongodb client pool done ..." << std::endl;
   	 	   if (mongodb_client_pool == nullptr) {
	 	        return EXIT_FAILURE;
    	          }
   	 	            
 
   std::cout << "Mongodb before client pop 11..." << std::endl;
    
   mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
      if (!mongodb_client) {
          LOG(fatal) << "Failed to pop mongoc client";
	        return EXIT_FAILURE;
	    }
//      std::cout << "Mongodb before client pop 22..." << std::endl;

       mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
	std::cout << "Mongodb client push done ..." << std::endl;


   auto collection = mongoc_client_get_collection(
         mongodb_client, "movies", "movie-info");
	  if (!collection) {
 	          ServiceException se;
  	          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
 	          se.message = "Failed to create collection user from DB recommender";
 	          mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  	          throw se;
 		 }

  	  std::cout << "Mongodb get coll done here !!!!!!!! ..." << std::endl;

//	  std::vector<std::string> movie_ids;
//	  std::vector<std::string> movie_titles;
//		movie_ids.push_back("MOV0001");
//		movie_ids.push_back("MOV0002");
//		movie_ids.push_back("MOV0003");
//		movie_ids.push_back("MOV0004");


	const char *ids[] = {"0001","0002"};
	const char *titles[] = {"Spiderman", "Batman returns"};
	uint32_t    i;
	char        buf[16];
	const       char *key;
	size_t      keylen;
//	bson_t     *document;
	bson_t      child;
	bson_t      child2;

	bson_t *document = bson_new ();

	BSON_APPEND_ARRAY_BEGIN (document, "movies", &child);
	   for (i = 0; i < sizeof ids / sizeof (char *); ++i) {
	      keylen = bson_uint32_to_string (i, &key, buf, sizeof buf);
	      bson_append_document_begin (&child, key, (int) keylen, &child2);
	      BSON_APPEND_UTF8 (&child2, "movie_id", ids[i]);
	      BSON_APPEND_UTF8 (&child2, "title", titles[i]);
	      bson_append_document_end (&child, &child2);
	    }
	      bson_append_array_end (document, &child);

	   bson_error_t error;
           bool movieinsert = mongoc_collection_insert_one (collection, document, nullptr, nullptr, &error);

	   if (!movieinsert) {
		    std::cout << "INSERT FAILED **************************..." << std::endl;
		    ServiceException se;
                     se.errorCode = ErrorCode::SE_MONGODB_ERROR;
                     se.message = error.message;
                      bson_destroy(document);
		       throw se;

	   }

	   std::cout << "INSEERT NO FAIL !!!!!!! ..." << std::endl;
	   bson_t *movie_doc1 = bson_new();
          BSON_APPEND_INT64(movie_doc1, "movie_id", 789);
          BSON_APPEND_UTF8(movie_doc1, "title", "Spiderman I");
	  BSON_APPEND_UTF8(movie_doc1, "movie_url", "/home/ubuntu/vishh/newmongo/videos/AOSTest.mp4");
	  std::cout << "data title2 insert done !!!!!!! ..." << std::endl;

	 // bson_error_t error;
  	  bool movieinsert1 = mongoc_collection_insert_one (collection, movie_doc1, nullptr, nullptr, &error);
         std::cout << "Mongodb after insert2 here !!!!!!!! ..." << std::endl;

	 if (!movieinsert1) {
         // LOG(error) << "Failed to insert Movies for " << std::to_string(movie_id) << " to MongoDB: " << error.message;
	               ServiceException se;
	               se.errorCode = ErrorCode::SE_MONGODB_ERROR;
	              se.message = error.message;
	              bson_destroy(movie_doc1);
	              //  mongoc_cursor_destroy(cursor);
	             //  mongoc_collection_destroy(collection);
	            //mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
	                throw se;
	                 }
	              bson_destroy(movie_doc1);
	        	  std::cout << "Mongodb after destroy done here !!!!!!!! ..." << std::endl;
	      	

  // 4: configure this server
  TThreadedServer server(
      std::make_shared<MovieInfoServiceProcessor>(
      std::make_shared<MovieInfoServiceHandler>(mongodb_client_pool,mongodb_client)),
      std::make_shared<TServerSocket>("0.0.0.0", my_port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>()
  );
  
  // 5: start the server
  std::cout << "Starting the movie info server ..." << std::endl;
  server.serve();
  return 0;
}

