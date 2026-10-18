<?php

namespace MediaWiki\Extension\DelayedJobQueue\Tests\Integration;

use MediaWiki\Extension\DelayedJobQueue\DelayedJobQueueDB;
use MediaWiki\JobQueue\JobQueue;
use MediaWiki\JobQueue\JobQueueGroup;
use MediaWiki\JobQueue\JobSpecification;
use MediaWiki\WikiMap\WikiMap;
use MediaWikiIntegrationTestCase;
use Wikimedia\Timestamp\ConvertibleTimestamp;

/**
 * @covers \MediaWiki\Extension\DelayedJobQueue\DelayedJobQueueDB
 * @group Database
 */
class DelayedJobQueueDBTest extends MediaWikiIntegrationTestCase {

	private const NOW = 1_800_000_000;

	protected function setUp(): void {
		parent::setUp();
		ConvertibleTimestamp::setFakeTime( self::NOW );
	}

	private function newQueue(): DelayedJobQueueDB {
		return JobQueue::factory( [
			'class' => DelayedJobQueueDB::class,
			'domain' => WikiMap::getCurrentWikiDbDomain()->getId(),
			'type' => 'null',
			'idGenerator' => $this->getServiceContainer()->getGlobalIdGenerator(),
			'claimTTL' => 3600,
		] );
	}

	private function newJob( ?int $release, array $params = [], array $opts = [] ): JobSpecification {
		if ( $release !== null ) {
			$params['jobReleaseTimestamp'] = $release;
		}
		return new JobSpecification( 'null', $params + [ 'id' => mt_rand() ], $opts );
	}

	public function testDelayedJobWaitsUntilItsReleaseTime() {
		$queue = $this->newQueue();
		$queue->push( $this->newJob( self::NOW + 60 ) );

		$this->assertSame( 1, $queue->getDelayedCount() );
		$this->assertTrue( $queue->isEmpty() );
		$this->assertFalse( $queue->pop() );

		ConvertibleTimestamp::setFakeTime( self::NOW + 60 );
		$this->assertFalse( $queue->isEmpty() );
		$this->assertNotFalse( $queue->pop() );
		$this->assertSame( 0, $queue->getDelayedCount() );
	}

	public function testJobWithoutReleaseTimeIsQueuedAtOnce() {
		$queue = $this->newQueue();
		$queue->push( $this->newJob( null ) );
		$queue->push( $this->newJob( self::NOW - 1 ) );

		$this->assertSame( 0, $queue->getDelayedCount() );
		$this->assertSame( 2, $queue->getSize() );
	}

	public function testDuplicatesWaitOnce() {
		$queue = $this->newQueue();
		$params = [ 'id' => 1 ];
		$queue->push( $this->newJob( self::NOW + 60, $params, [ 'removeDuplicates' => true ] ) );
		$queue->push( $this->newJob( self::NOW + 90, $params, [ 'removeDuplicates' => true ] ) );

		$this->assertSame( 1, $queue->getDelayedCount() );
	}

	public function testDueJobIsSeenByTheQueueGroup() {
		$this->overrideConfigValue( 'JobTypeConf', [
			'default' => [ 'class' => DelayedJobQueueDB::class, 'claimTTL' => 3600 ],
		] );
		$group = $this->getServiceContainer()->getJobQueueGroup();
		$group->push( $this->newJob( self::NOW + 60 ) );

		$this->assertNotContains( 'null', $group->getQueuesWithJobs() );
		ConvertibleTimestamp::setFakeTime( self::NOW + 60 );
		$this->assertContains( 'null', $group->getQueuesWithJobs() );
		$this->assertNotFalse( $group->pop( JobQueueGroup::TYPE_DEFAULT ) );
	}

	public function testDeleteDropsDelayedJobs() {
		$queue = $this->newQueue();
		$queue->push( $this->newJob( self::NOW + 60 ) );
		$queue->delete();

		$this->assertSame( 0, $queue->getDelayedCount() );
	}
}
