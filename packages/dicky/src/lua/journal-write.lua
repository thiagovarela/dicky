-- KEYS[1] = journal hash key
-- ARGV[1] = sequence
-- ARGV[2] = entry JSON

local key = KEYS[1]
local sequence = ARGV[1]
local entry = ARGV[2]

return redis.call('HSETNX', key, sequence, entry)
