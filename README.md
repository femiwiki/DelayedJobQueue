# DelayedJobQueue

A MediaWiki extension that lets the database job queue (`JobQueueDB`) hold
jobs until their release time, as the Redis and EventBus queues do.

Without it, MediaWiki on the default queue cannot use features that need
delayed jobs, such as
[`$wgCdnReboundPurgeDelay`](https://www.mediawiki.org/wiki/Manual:$wgCdnReboundPurgeDelay)
or GrowthExperiments' notifications to new editors.

## How it works

`DelayedJobQueueDB` extends `JobQueueDB`. A job with a release time in the
future waits in the `delayed_job` table. Each time a job runner looks at the
queue, due jobs move to the `job` table and run as usual, so they run with the
first `runJobs` after their release time.

## Installation

```php
wfLoadExtension( 'DelayedJobQueue' );
$wgJobTypeConf['default'] = [
	'class' => \MediaWiki\Extension\DelayedJobQueue\DelayedJobQueueDB::class,
	'order' => 'random',
	'claimTTL' => 3600,
];
```

Then run `maintenance/run.php update` to create the `delayed_job` table.

To send a second purge to the CDN after an edit, as Wikimedia does:

```php
$wgCdnReboundPurgeDelay = 11;
```

## License

GPL-2.0-or-later
