-- KEYS[1] = timers sorted set key
-- ARGV[1] = current timestamp (now)
-- ARGV[2] = limit

local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local expired = {}
local members = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)

for i = 1, #members, 2 do
  local member = members[i]

  if redis.call('ZREM', key, member) == 1 then
    table.insert(expired, member)
  end
end

return expired
