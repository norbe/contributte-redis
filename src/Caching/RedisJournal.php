<?php declare(strict_types = 1);

namespace Contributte\Redis\Caching;

use Nette\Caching\Cache;
use Nette\Caching\Storages\Journal;
use Predis\Client;
use Predis\ClientInterface;

/**
 * @see based on original https://github.com/Kdyby/Redis
 */
final class RedisJournal implements Journal
{

	public const NS_PREFIX = 'Contributte.Journal';

	public const KEY_PRIORITY = 'priority';
	public const SUFFIX_TAGS = 'tags';
	public const SUFFIX_KEYS = 'keys';

	private const CLEAN_CHUNK_SIZE = 1000;

	/** @var ClientInterface $client */
	private $client;

	public function __construct(ClientInterface $client)
	{
		$this->client = $client;
	}

	/**
	 * Writes entry information into the journal.
	 *
	 * @param array{tags: string[], priority: int} $dependencies
	 */
	public function write(string $key, array $dependencies): void
	{
		$this->cleanEntry($key);

		$this->client->multi();

		// add entry to each tag & tag to entry
		$tags = !isset($dependencies[Cache::Tags]) ? [] : (array) $dependencies[Cache::Tags];
		foreach (array_unique($tags) as $tag) {
			$this->client->sadd($this->formatKey($tag, self::SUFFIX_KEYS), [$key]);
			$this->client->sadd($this->formatKey($key, self::SUFFIX_TAGS), [$tag]);
		}

		if (isset($dependencies[Cache::Priority])) {
			$this->client->zadd($this->formatKey(self::KEY_PRIORITY), [$key => (int) $dependencies[Cache::Priority]]);
		}

		$this->client->exec();
	}

	/**
	 * Deletes all keys from associated tags and all priorities
	 *
	 * @param mixed[]|string $keys
	 */
	public function cleanEntry(array|string $keys): void
	{
		foreach (is_array($keys) ? $keys : [$keys] as $key) {
			$entries = $this->entryTags((string) $key);

			$this->client->multi();
			foreach ($entries as $tag) {
				$this->client->srem($this->formatKey((string) $tag, self::SUFFIX_KEYS), (string) $key);
			}

			// drop tags of entry and priority, in case there are some
			$this->client->del($this->formatKey((string) $key, self::SUFFIX_TAGS));
			$this->client->zrem($this->formatKey(self::KEY_PRIORITY), (string) $key);

			$this->client->exec();
		}
	}

	/**
	 * Cleans entries from journal.
	 *
	 * @param mixed[] $conditions
	 * @return mixed[] of removed items or NULL when performing a full cleanup
	 */
	public function clean(array $conditions): ?array
	{
		if (isset($conditions[Cache::All])) {
			$all = $this->client->keys(self::NS_PREFIX . ':*');

			$this->client->multi();
			call_user_func_array([$this->client, 'del'], $all);
			$this->client->exec();

			return null;
		}

		$entries = [];
		if (isset($conditions[Cache::Tags])) {
			$entries = $this->tagsEntries(array_map('strval', (array) $conditions[Cache::Tags]));
		}

		if (isset($conditions[Cache::Priority])) {
			$entries = array_merge($entries, $this->priorityEntries($conditions[Cache::Priority]));
		}

		$entries = array_values(array_unique($entries));
		$this->cleanEntries($entries);

		return $entries;
	}

	/**
	 * Removes many entries from the journal in a few round trips. Cleaning them one by one costs
	 * several commands per entry, which takes tens of seconds once the cleaned tags hold tens of thousands of entries.
	 *
	 * @param mixed[] $keys
	 */
	private function cleanEntries(array $keys): void
	{
		if (!$this->client instanceof Client) {
			$this->cleanEntry($keys);

			return;
		}

		foreach (array_chunk($keys, self::CLEAN_CHUNK_SIZE) as $chunk) {
			$chunk = array_map('strval', $chunk);
			$tagsOfEntries = $this->client->pipeline(function ($pipeline) use ($chunk): void {
				foreach ($chunk as $key) {
					$pipeline->smembers($this->formatKey($key, self::SUFFIX_TAGS));
				}
			});

			$entriesByTag = [];
			foreach ($chunk as $index => $key) {
				foreach ((array) $tagsOfEntries[$index] as $tag) {
					$entriesByTag[$tag][] = $key;
				}
			}

			$this->client->pipeline(['atomic' => true], function ($pipeline) use ($chunk, $entriesByTag): void {
				foreach ($entriesByTag as $tag => $entries) {
					$pipeline->srem($this->formatKey((string) $tag, self::SUFFIX_KEYS), $entries);
				}

				$pipeline->del(array_map(fn (string $key): string => $this->formatKey($key, self::SUFFIX_TAGS), $chunk));
				$pipeline->zrem($this->formatKey(self::KEY_PRIORITY), $chunk);
			});
		}
	}

	/**
	 * @param string[] $tags
	 * @return mixed[]
	 */
	private function tagsEntries(array $tags): array
	{
		if (!$this->client instanceof Client) {
			return array_merge([], ...array_map(fn (string $tag): array => $this->tagEntries($tag), $tags));
		}

		$entries = [];
		foreach (array_chunk($tags, self::CLEAN_CHUNK_SIZE) as $chunk) {
			$found = $this->client->pipeline(function ($pipeline) use ($chunk): void {
				foreach ($chunk as $tag) {
					$pipeline->smembers($this->formatKey($tag, self::SUFFIX_KEYS));
				}
			});
			foreach ($found as $tagEntries) {
				$entries[] = (array) $tagEntries;
			}
		}

		return array_merge([], ...$entries);
	}

	/**
	 * @return mixed[]
	 */
	private function priorityEntries(int $priority): array
	{
		$result = $this->client->zrangebyscore($this->formatKey(self::KEY_PRIORITY), 0, $priority);

		return $result !== null ? (array) $result : [];
	}

	/**
	 * @return mixed[]
	 */
	private function entryTags(string $key): array
	{
		$result = $this->client->smembers($this->formatKey($key, self::SUFFIX_TAGS));

		return $result !== null ? (array) $result : [];
	}

	/**
	 * @return mixed[]
	 */
	private function tagEntries(string $tag): array
	{
		$result = $this->client->smembers($this->formatKey($tag, self::SUFFIX_KEYS));

		return $result !== null ? (array) $result : [];
	}

	private function formatKey(string $key, ?string $suffix = null): string
	{
		return self::NS_PREFIX . ':' . $key . ($suffix !== null ? ':' . $suffix : '');
	}

}
