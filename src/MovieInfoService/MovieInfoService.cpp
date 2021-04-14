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
   	 	            
    
   mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
      if (!mongodb_client) {
          LOG(fatal) << "Failed to pop mongoc client";
	        return EXIT_FAILURE;
	    }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

   auto collection = mongoc_client_get_collection(
         mongodb_client, "movies", "movie-info");
	  if (!collection) {
 	          ServiceException se;
  	          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
 	          se.message = "Failed to create collection user from DB recommender";
 	          mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  	          throw se;
 		 }


	 mongoc_bulk_operation_t *bulk;
 	 bson_error_t error;
	 bool ret;
	 int i;

	 std::vector<std::string> movie_ids{"MOV0001","MOV0002","MOV0003","MOV0004"};
	 std::vector<std::string> movie_titles{"Avengers","Catwoman","Batman","Spiderman"};

	 bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

	 for (i = 0; i < 4; i++) {
	  bson_t *movie_doc = bson_new();
	  std::string &id = movie_ids[i];
	  std::string &title = movie_titles[i];
	  BSON_APPEND_UTF8(movie_doc, "movie_id", id.c_str());
          BSON_APPEND_UTF8(movie_doc, "movie_title", title.c_str());
	  
	  mongoc_bulk_operation_insert (bulk, movie_doc);
	  std::cout << "INSEERT **************************** "<< title.c_str() << " done" <<std::endl;
	  bson_destroy(movie_doc);
	 }  
	     std::cout << "BEFORE BULK DONE !!!!!!! ..." << std::endl;

	   ret = mongoc_bulk_operation_execute (bulk, NULL, &error);
	    if (!ret) {
                  std::cout << "Movies data Insert failed ..." << std::endl;
		  ServiceException se;
                  se.errorCode = ErrorCode::SE_MONGODB_ERROR;
                  se.message = error.message;
		  throw se;
                  }
              std::cout << "DATA BULK INSERT DONE0 !!!!!!!! ..." << std::endl;

              mongoc_collection_destroy(collection);
              mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
              mongoc_cleanup ();
              
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

