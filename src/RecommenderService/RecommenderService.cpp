#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <signal.h>

#include "../utils.h"
#include "../utils_mongodb.h"
#include "RecommenderHandler.h"

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
  // Notify the singal handler if interrupted
  signal(SIGINT, sigintHandler);
  // Initialize logging
  init_logger();


  // Read the config file for ports and addresses
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  // Get my port
  int my_port = config_json["recommender-service"]["port"];

  // Get the movie info service's port and address
  int movie_info_service_port = config_json["movie-info-service"]["port"];
  std::string movie_info_service_addr = config_json["movie-info-service"]["addr"];
 
  // Get the client of movie-info-service
  ClientPool<ThriftClient<MovieInfoServiceClient>> movie_info_client_pool(
      "movie-info-service", movie_info_service_addr, movie_info_service_port, 0, 128, 1000);
	  
  // Get the user likes service's port and address
  int user_likes_service_port = config_json["user-likes-service"]["port"];
  std::string user_likes_service_addr = config_json["user-likes-service"]["addr"];
 
  // Get the client of user-likes-service
  ClientPool<ThriftClient<UserLikesServiceClient>> user_likes_client_pool(
      "user-likes-service", user_likes_service_addr, user_likes_service_port, 0, 128, 1000);

  // Get mongodb client pool
  mongoc_client_pool_t* mongodb_client_pool =
     init_mongodb_client_pool(config_json, "recommender", 128);

  if (mongodb_client_pool == nullptr) {
     return EXIT_FAILURE;
  }

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
    if (!mongodb_client) {
      LOG(fatal) << "Failed to pop mongoc client";
      return EXIT_FAILURE;
    }
    mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  // Configure this server
  TThreadedServer server(
      std::make_shared<RecommenderServiceProcessor>(
          std::make_shared<RecommenderServiceHandler>(
              mongodb_client_pool,
              &movie_info_client_pool, &user_likes_client_pool)),
      std::make_shared<TServerSocket>("0.0.0.0", my_port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>()
  );
  
  // Start the server
  std::cout << "Starting the recommender server ..." << std::endl;
  server.serve();
  return 0;
}

