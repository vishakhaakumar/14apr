local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.Upload()
 local ngx = ngx
 local MovieInfoServiceClient = require "movies_MovieInfoService"
 local GenericObjectPool = require "GenericObjectPool"
  -- Read the parameters sent by the end user client
  
  ngx.req.read_body()
        local post = ngx.req.get_post_args()

        if (_StrIsEmpty(post.movie_name) ) then
           ngx.status = ngx.HTTP_BAD_REQUEST
           ngx.say("Incomplete arguments")
           ngx.log(ngx.ERR, "Incomplete arguments")
           ngx.exit(ngx.HTTP_BAD_REQUEST)
        end

  ngx.say("Inside Nginx Lua script: Processing Get Movie list... Request from: ", post.movie_name)
  local client = GenericObjectPool:connection(MovieInfoServiceClient, "movie-info-service", 9093)  
  local movie_ids = {"MOV0001","MOV0002","MOV0003","MOV0004"};
  local movie_titles = {"Avengers","Catwoman","Batman","Spiderman"};
  local movie_links = {"a","b","c","d"};

  local status, ret = pcall(client.UploadMovies, client, movie_ids, movie_titles, movie_links)
  GenericObjectPool:returnConnection(client)
  ngx.say("Status: ",status)

  if not status then

        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        if (ret.message) then
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to get Movie names: " .. ret.message)
            ngx.log(ngx.ERR, "Failed to get Movie names: " .. ret.message)
        else
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to get Movie names: " )
            ngx.log(ngx.ERR, "Failed to get Movie names: " )
        end
        ngx.exit(ngx.HTTP_OK)
    else
	ngx.header.content_type = "text/plain"
	ngx.say("message is: ", ret)
    	ngx.exit(ngx.HTTP_OK)
    end

end

return _M
