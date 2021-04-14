#ifndef MOVIES_MICROSERVICES_USERLIKESHANDLER_H
#define MOVIES_MICROSERVICES_USERLIKESHANDLER_H

#include <iostream>
#include <string>
#include <regex>
#include <future>

#include "../../gen-cpp/UserLikesService.h"

#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"

namespace movies{

class UserLikesServiceHandler : public UserLikesServiceIf {
 public:
  UserLikesServiceHandler();
  ~UserLikesServiceHandler() override=default;

  void GetMovieLikesByIds(std::vector<int64_t>& _return, const std::vector<std::string> & movie_ids) override;
  void LikeDislikeMovie(const std::string& user_id, const std::string& movie_id, const bool likeDislike) override;
};

// Constructor
UserLikesServiceHandler::UserLikesServiceHandler() {

}

// Remote Procedure "GetMoviesLikesByIds"
void UserLikesServiceHandler::GetMovieLikesByIds(std::vector<int64_t>& _return, const std::vector<std::string> & movie_ids) {
	_return.push_back(-5);
	_return.push_back(8);
}
// Remote Procedure "LikeDislikeMovie"
void UserLikesServiceHandler::LikeDislikeMovie(const std::string& user_id, const std::string& movie_id, const bool likeDislike) {
	// TO DO: update database with user like or dislike (false == dislike, true == like)
	if (user_id != "") {
		// adjust like or dislike
	}
	else {
		return;
	}
}

} // namespace movies


#endif //MOVIES_MICROSERVICES_USERLIKESHANDLER_H

