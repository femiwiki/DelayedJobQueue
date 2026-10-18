<?php

namespace MediaWiki\Extension\DelayedJobQueue;

use MediaWiki\JobQueue\IJobSpecification;
use MediaWiki\JobQueue\JobQueueDB;
use Wikimedia\Rdbms\DBError;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Timestamp\ConvertibleTimestamp;

/**
 * The database job queue, plus jobs with a release time. Such a job waits in
 * the delayed_job table and moves to the job table once it is due, the next
 * time the queue is looked at.
 */
class DelayedJobQueueDB extends JobQueueDB {
	/** Due jobs moved per look at the queue */
	private const RELEASE_BATCH = 100;
	/** Same range as JobQueueDB uses for job_random */
	private const MAX_JOB_RANDOM = 2_147_483_647;

	/** @inheritDoc */
	protected function supportsDelayedJobs() {
		return true;
	}

	/** @inheritDoc */
	protected function doBatchPush( array $jobs, $flags ) {
		$now = (int)ConvertibleTimestamp::time();
		$ready = [];
		$delayed = [];
		foreach ( $jobs as $job ) {
			if ( $job->getReleaseTimestamp() > $now ) {
				$delayed[] = $job;
			} else {
				$ready[] = $job;
			}
		}
		if ( $ready ) {
			parent::doBatchPush( $ready, $flags );
		}
		if ( $delayed ) {
			// Written when the job table would be, see JobQueueDB::doBatchPush()
			$dbw = $this->getPrimaryDB();
			$fname = __METHOD__;
			$dbw->onTransactionPreCommitOrIdle(
				fn () => $this->insertDelayed( $dbw, $delayed, $fname ),
				$fname
			);
		}
	}

	/**
	 * @param IDatabase $dbw
	 * @param IJobSpecification[] $jobs
	 * @param string $method
	 */
	private function insertDelayed( IDatabase $dbw, array $jobs, string $method ): void {
		$rows = [];
		$dedupe = [];
		foreach ( $jobs as $job ) {
			$fields = $this->insertFields( $job, $dbw );
			if ( $job->ignoreDuplicates() ) {
				if ( isset( $dedupe[$fields['job_sha1']] ) ) {
					continue;
				}
				$dedupe[$fields['job_sha1']] = count( $rows );
			}
			$rows[] = [
				'dj_cmd' => $fields['job_cmd'],
				'dj_namespace' => $fields['job_namespace'],
				'dj_title' => $fields['job_title'],
				'dj_params' => $fields['job_params'],
				'dj_sha1' => $fields['job_sha1'],
				'dj_dedupe' => $job->ignoreDuplicates() ? 1 : 0,
				'dj_release' => $dbw->timestamp( $job->getReleaseTimestamp() ),
			];
		}
		try {
			// A job that ignores duplicates is dropped if its twin is already waiting
			if ( $dedupe ) {
				$waiting = $dbw->newSelectQueryBuilder()
					->select( 'dj_sha1' )
					->from( 'delayed_job' )
					->where( [ 'dj_sha1' => array_map( 'strval', array_keys( $dedupe ) ), 'dj_dedupe' => 1 ] )
					->caller( $method )->fetchFieldValues();
				foreach ( $waiting as $sha1 ) {
					unset( $rows[$dedupe[$sha1]] );
				}
			}
			foreach ( array_chunk( array_values( $rows ), 50 ) as $batch ) {
				$dbw->newInsertQueryBuilder()
					->insertInto( 'delayed_job' )
					->rows( $batch )
					->caller( $method )->execute();
			}
		} catch ( DBError $e ) {
			throw $this->getDBException( $e );
		}
	}

	/**
	 * Move the due jobs of the given types to the job table.
	 *
	 * @param string[] $types
	 */
	private function releaseDueJobs( array $types ): void {
		$dbw = $this->getPrimaryDB();
		try {
			$due = $dbw->newSelectQueryBuilder()
				->select( '*' )
				->from( 'delayed_job' )
				->where( [
					'dj_cmd' => $types,
					$dbw->expr( 'dj_release', '<=', $dbw->timestamp() ),
				] )
				->orderBy( 'dj_release' )
				->limit( self::RELEASE_BATCH )
				->caller( __METHOD__ )->fetchResultSet();
			foreach ( $due as $row ) {
				$dbw->startAtomic( __METHOD__ );
				// Whoever deletes the row moves it, so two runners never both do
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( 'delayed_job' )
					->where( [ 'dj_id' => $row->dj_id ] )
					->caller( __METHOD__ )->execute();
				if ( $dbw->affectedRows() && !( $row->dj_dedupe && $this->isQueued( $dbw, $row->dj_sha1 ) ) ) {
					$dbw->newInsertQueryBuilder()
						->insertInto( 'job' )
						->row( [
							'job_cmd' => $row->dj_cmd,
							'job_namespace' => $row->dj_namespace,
							'job_title' => $row->dj_title,
							'job_params' => $row->dj_params,
							'job_timestamp' => $dbw->timestamp(),
							'job_sha1' => $row->dj_sha1,
							'job_random' => mt_rand( 0, self::MAX_JOB_RANDOM ),
						] )
						->caller( __METHOD__ )->execute();
				}
				$dbw->endAtomic( __METHOD__ );
			}
		} catch ( DBError $e ) {
			throw $this->getDBException( $e );
		}
	}

	/**
	 * Whether an unclaimed job with this hash waits in the job table, the
	 * check JobQueueDB makes for a job that ignores duplicates.
	 */
	private function isQueued( IDatabase $dbw, string $sha1 ): bool {
		return (bool)$dbw->newSelectQueryBuilder()
			->select( '1' )
			->from( 'job' )
			->where( [ 'job_sha1' => $sha1, 'job_token' => '' ] )
			->caller( __METHOD__ )->fetchField();
	}

	/** @inheritDoc */
	protected function doIsEmpty() {
		$this->releaseDueJobs( [ $this->type ] );
		return parent::doIsEmpty();
	}

	/** @inheritDoc */
	protected function doPop() {
		$this->releaseDueJobs( [ $this->type ] );
		return parent::doPop();
	}

	/** @inheritDoc */
	protected function doGetSiblingQueuesWithJobs( array $types ) {
		$this->releaseDueJobs( $types );
		return parent::doGetSiblingQueuesWithJobs( $types );
	}

	/** @inheritDoc */
	protected function doGetDelayedCount() {
		try {
			return $this->getReplicaDB()->newSelectQueryBuilder()
				->from( 'delayed_job' )
				->where( [ 'dj_cmd' => $this->type ] )
				->caller( __METHOD__ )->fetchRowCount();
		} catch ( DBError $e ) {
			throw $this->getDBException( $e );
		}
	}

	/** @inheritDoc */
	protected function doDelete() {
		try {
			$this->getPrimaryDB()->newDeleteQueryBuilder()
				->deleteFrom( 'delayed_job' )
				->where( [ 'dj_cmd' => $this->type ] )
				->caller( __METHOD__ )->execute();
		} catch ( DBError $e ) {
			throw $this->getDBException( $e );
		}
		return parent::doDelete();
	}
}
