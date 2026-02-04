-- KEYS[1] = lock key
-- ARGV[1] = lock token
-- ARGV[2] = TTL in milliseconds

local key = KEYS[1]
local token = ARGV[1]
local ttlMs = ARGV[2]

local result = redis.call('SET', key, token, 'PX', ttlMs, 'NX')

if result then
  return 1
end

return 0
