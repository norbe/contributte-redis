<?php declare(strict_types = 1);

/**
 * A clean of the tags of an entry that runs while the entry is being written must never leave the entry
 * in the cache without its journal: no later clean of its tags could remove it then.
 */

namespace Tests\Cases\Caching;

use Contributte\Redis\Caching\ExclusiveLock;
use Contributte\Redis\Caching\RedisJournal;
use Contributte\Redis\Caching\RedisStorage;
use Nette\Caching\Cache;
use Predis\Client;
use Predis\Connection\ConnectionException;
use Tester\Assert;
use Tester\Environment;

foreach ([__DIR__ . '/../../../vendor/autoload.php', __DIR__ . '/../../../../../autoload.php'] as $autoload) {
	if (is_file($autoload)) {
		require_once $autoload;
		break;
	}
}

Environment::setup();

/**
 * Runs a callback right before the first data SET it sends, the moment a clean running in another
 * process could slip between the write of the journal and the write of the entry.
 */
final class CleanBeforeSetClient extends Client
{

	/** @var callable|null */
	public $beforeSet;

	public function __call($commandID, $arguments)
	{
		if (strtolower($commandID) === 'set' && $this->beforeSet !== null) {
			$beforeSet = $this->beforeSet;
			$this->beforeSet = null;
			$beforeSet();
		}

		return parent::__call($commandID, $arguments);
	}

}

$parameters = ['host' => '127.0.0.1', 'port' => 6379, 'database' => 2];
$options = ['prefix' => 'contributte_redis_test_' . bin2hex(random_bytes(4)) . ':'];

try {
	$writerClient = new CleanBeforeSetClient($parameters, $options);
	$writerClient->ping();
	$cleanerClient = new Client($parameters, $options);
} catch (ConnectionException $e) {
	Environment::skip('Redis not found: ' . $e->getMessage());
}

$writer = new Cache(new RedisStorage($writerClient, new ExclusiveLock($writerClient), new RedisJournal($writerClient)), 'test');
$cleaner = new Cache(new RedisStorage($cleanerClient, new ExclusiveLock($cleanerClient), new RedisJournal($cleanerClient)), 'test');

$writer->save('free_pcs', 1, [Cache::Tags => ['product-1']]);
$cleaner->clean([Cache::Tags => ['product-1']]);
Assert::null($cleaner->load('free_pcs'));

$writerClient->beforeSet = function () use ($cleaner): void {
	$cleaner->clean([Cache::Tags => ['product-1']]);
};
$writer->save('free_pcs', 1, [Cache::Tags => ['product-1']]);

$cleaner->clean([Cache::Tags => ['product-1']]);
Assert::null($cleaner->load('free_pcs'), 'An entry written while its tag was being cleaned stays cleanable by the tag');

$writer->save('free_pcs', 2, [Cache::Tags => ['product-1']]);
$writer->save('free_pcs', 3, [Cache::Tags => ['product-2']]);
$cleaner->clean([Cache::Tags => ['product-1']]);
Assert::same(3, $cleaner->load('free_pcs'), 'A rewrite drops the tags of the previous version of the entry');
$cleaner->clean([Cache::Tags => ['product-2']]);
Assert::null($cleaner->load('free_pcs'));

foreach ($cleanerClient->keys('*') as $key) {
	$cleanerClient->del([substr($key, strlen($options['prefix']))]);
}
