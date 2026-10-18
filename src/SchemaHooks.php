<?php

namespace MediaWiki\Extension\DelayedJobQueue;

use MediaWiki\Installer\Hook\LoadExtensionSchemaUpdatesHook;

class SchemaHooks implements LoadExtensionSchemaUpdatesHook {

	/** @inheritDoc */
	public function onLoadExtensionSchemaUpdates( $updater ) {
		$dir = dirname( __DIR__ ) . '/sql';
		$type = $updater->getDB()->getType();
		$updater->addExtensionTable( 'delayed_job', "$dir/$type/tables-generated.sql" );
	}
}
