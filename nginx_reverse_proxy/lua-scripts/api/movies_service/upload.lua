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

 -- ngx.say("Inside Nginx Lua script: Processing Get Movie list... Request from: ", post.movie_name)
  local client = GenericObjectPool:connection(MovieInfoServiceClient, "movie-info-service", 9093)  
  local movie_ids = {"MOV00011","MOV00022","MOV00033","MOV00044"};
  local movie_titles = {"Avengers I","Catwoman I","Batman I","Spiderman I"};
  local movie_links = {"a","b","c","d"};

  local status, ret = pcall(client.UploadMovies, client, movie_ids, movie_titles, movie_links)
  GenericObjectPool:returnConnection(client)
  ngx.say("Status: ",status)

  if not status then

        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        if (ret.message) then
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to upload Movies: " .. ret.message)
            ngx.log(ngx.ERR, "Failed to upload Movies: " .. ret.message)
        else
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to uploadt Movies: " )
            ngx.log(ngx.ERR, "Failed to upload Movies: " )
        end
        ngx.exit(ngx.HTTP_OK)
    else
	ngx.header.content_type = "text/plain"
	ngx.say(ret)
    	ngx.exit(ngx.HTTP_OK)
    end

end

return _M
