-- KEYS[1] = lock key
-- ARGV[1] = lock token

local key = KEYS[1]
local token = ARGV[1]

local current = redis.call('GET', key)

if current == token then
  redis.call('DEL', key)
  return 1
end

return 0
