local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.Get()
  local RecommenderServiceClient = require "movies_RecommenderService"
  local GenericObjectPool = require "GenericObjectPool"
  local ngx = ngx
  -- Read the parameters sent by the end user client
  
  ngx.req.read_body()
        local post = ngx.req.get_post_args()

        if (_StrIsEmpty(post.user_id) ) then
           ngx.status = ngx.HTTP_BAD_REQUEST
           ngx.say("Incomplete arguments")
           ngx.log(ngx.ERR, "Incomplete arguments")
           ngx.exit(ngx.HTTP_BAD_REQUEST)
        end

  ngx.say("Inside Nginx Lua script: Processing Get Recommendations... Request from: ", post.user_id)
  
  local client = GenericObjectPool:connection(RecommenderServiceClient, "recommender-service", 9092)
  local status, ret = pcall(client.GetRecommendations, client, post.user_id)
  GenericObjectPool:returnConnection(client)
  ngx.say("Status: ", status)

  if not status then
    
        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        if (ret.message) then
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to get recommendations: " .. ret.message)
            ngx.log(ngx.ERR, "Failed to get recommendations: " .. ret.message)
        else
            ngx.header.content_type = "text/plain"
            ngx.say("Failed to get recommendations: " )
            ngx.log(ngx.ERR, "Failed to get recommendations: " )
        end
        ngx.exit(ngx.HTTP_OK)
    else
        ngx.header.content_type = "text/plain"
        if (table.getn(ret) > 0) then
            -- Format the returned list of movies so that each appears on a new line
            local str = ""
            for k, v in ipairs(ret) do
                str = str .. v .. "\n"
            end
            ngx.say("Recommendations:\n", str)
        else
            ngx.say("There are no recommendations for this user")
        end
        ngx.exit(ngx.HTTP_OK)
    end

end

return _M
