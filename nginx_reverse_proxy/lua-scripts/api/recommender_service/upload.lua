local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.Upload()
  local RecommenderServiceClient = require "movies_RecommenderService"
  local GenericObjectPool = require "GenericObjectPool"
  local ngx = ngx
  -- Read the parameters sent by the end user client
  
  ngx.req.read_body()
        local post = ngx.req.get_post_args()

        if (_StrIsEmpty(post.user_id) or _StrIsEmpty(post.movie_id)) then
           ngx.status = ngx.HTTP_BAD_REQUEST
           ngx.say("Incomplete arguments")
           ngx.log(ngx.ERR, "Incomplete arguments")
           ngx.exit(ngx.HTTP_BAD_REQUEST)
        end

  ngx.say("Inside Nginx Lua script: Processing Upload Recommendations... Request from: ", post.user_id)
  
  local client = GenericObjectPool:connection(RecommenderServiceClient, "recommender-service", 9092)

  -- If post.movie_id is a string, convert it to a list before passing it to
  -- RecommenderServiceClient.UploadRecommendations

  local movie_ids = {}

  if type(post.movie_id) == 'string' then
    movie_ids = {post.movie_id}
  else
    movie_ids = post.movie_id
  end

  for i,v in ipairs(movie_ids) do ngx.say("Movie id: ", v) end

  local status, ret = pcall(client.UploadRecommendations, client, post.user_id, movie_ids)
  GenericObjectPool:returnConnection(client)
  ngx.say("Status: ", status)

  if not status then
    
        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        if (ret.message) then
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to upload recommendations: " .. ret.message)
            ngx.log(ngx.ERR, "Failed to upload recommendations: " .. ret.message)
        else
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to upload recommendations: " )
            ngx.log(ngx.ERR, "Failed to upload recommendations: " )
        end
        ngx.exit(ngx.HTTP_OK)
    else
        ngx.header.content_type = "text/plain"
        ngx.say("Uploaded")
        ngx.exit(ngx.HTTP_OK)
    end

end

return _M
