-- KEYS[1] = lock key
-- ARGV[1] = lock token
-- ARGV[2] = new TTL in milliseconds

local key = KEYS[1]
local token = ARGV[1]
local ttlMs = tonumber(ARGV[2])

local current = redis.call('GET', key)

if current == token then
  redis.call('PEXPIRE', key, ttlMs)
  return 1
end

return 0
