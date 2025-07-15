<?php

namespace Contributte\Redis\Caching;

use Nette\Utils\Random;
use Predis\ClientInterface;

class ExclusiveLock
{
	private string $token;
	private array $locks = [];

	const DefaultTtlMs = 30000,
		DefaultTimeoutMs = 60000,
		DefaultRetryDelayMs = 100;

	public function __construct(private ClientInterface $client)
	{
		$this->token = Random::generate(16);
	}

	public function acquireWithRetry(
		string $key,
		int $ttlMs = self::DefaultTtlMs,
		int $timeoutMs = self::DefaultTimeoutMs,
		int $retryDelayMs = self::DefaultRetryDelayMs
	): bool {
		if(isset($this->locks[$key])){
			if($this->extend($key, $ttlMs)){
				return true;
			} else {
				unset($this->locks[$key]);
			}
		}
		$start = microtime(true);

		while (true) {
			if ($this->acquire($key, $ttlMs)) {
				return true;
			}

			usleep($retryDelayMs * 1000);

			$elapsed = (microtime(true) - $start) * 1000;
			if ($elapsed > $timeoutMs) {
				return false; // timeout
			}
		}
	}

	public function acquire(string $lockKey, int $ttlMs = self::DefaultTtlMs): bool
	{
		$result = 'OK' === (string)$this->client->set($lockKey, $this->token, 'PX', $ttlMs, 'NX');
		if($result) {
			$this->locks[$lockKey] = true;
		}
		return $result;
	}

	public function release(string $lockKey): bool
	{
		if(!isset($this->locks[$lockKey])){
			return false;
		}
		// Release only when tokens are equals (we are still the holder of the lock)
		$script = <<<LUA
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
LUA;

		$result = $this->client->eval($script, 1, $lockKey, $this->token);
		unset($this->locks[$lockKey]);
		return $result === 1;
	}

	public function extend(string $lockKey, int $ttlMs): bool
	{
		if(!isset($this->locks[$lockKey])){
			return false;
		}
		$script = <<<LUA
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
LUA;

		$result = $this->client->eval($script, 1, $lockKey, $this->token, $ttlMs);
		return $result === 1;
	}
}