<?php
require 'Slim/Slim.php';
\Slim\Slim::registerAutoloader ();

require_once ('database.php');

/**
 * Middleware that is responsible for authentication.
 */
class Authentication extends \Slim\Middleware {
	private function authenticate($userId, $sessionToken) {
		if ($userId == - 1) { // guest login
			return true;
		} else {
			$users = DB::query ( 'SELECT id FROM users WHERE id = ? AND session = ?', $userId, $sessionToken );
			
			return count ( $users ) > 0;
		}
	}
	
	/**
	 * Checking for authentication and writing user data into the request.
	 */
	public function call() {
		// Get reference to application
		$app = $this->app;
		
		// authenticate
		$userId = intval ( $app->request->headers ["Php-Auth-User"] );
		$sessionToken = $app->request->headers ["Php-Auth-Pw"];
		
		if ($this->authenticate ( $userId, $sessionToken )) {
			$app->request->authentication = array (
					"loggedIn" => true,
					"userId" => $userId 
			);
			   
			// Run inner middleware and application
			
			$this->next->call ();
		} else {
			// Send error code
			$app->response ()->status ( 401 );
		}
	}
}

$app = new \Slim\Slim ();
$app->add ( new \Authentication () );

/**
 * Storage and retrieval of workflows.
 */
$app->group ( '/workflows', function () use($app) {
	/**
	 * Get a list of all workflow group names.
	 */
	$app->get ( '/', function () use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			echo new stdClass ();
		}
		$userId = $app->request->authentication ["userId"];
		
		$groups = DB::query ( 'SELECT id, name, changed FROM workflow_groups WHERE user_id = ?', $userId );
		
		$output = array ();
		foreach ( $groups as $group ) {
			$output [$group->id] = array (
					"name" => $group->name,
					"changed" => $group->changed 
			);
		}
		
		echo json_encode ( ( object ) $output );
	} );
	
	/**
	 * Get a specific workflow group.
	 */
	$app->get ( '/:workflowId', function ($workflowGroupId) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			echo new stdClass ();
		}
		$userId = $app->request->authentication ["userId"];
		
		$workflows = DB::query ( 'SELECT w.name, w.graph FROM workflows w JOIN workflow_groups g ON(g.id = w.workflow_group_id) WHERE g.user_id = ? AND g.id = ?', $userId, $workflowGroupId );
		
		$output = array ();
		foreach ( $workflows as $workflow ) {
			$output [] = array (
					"name" => $workflow->name,
					"query" => $workflow->graph 
			);
		}
		
		echo json_encode ( $output );
	} );
	
	/**
	 * Store a workflow group.
	 */
	$app->post ( '/:name', function ($name) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$workflows = $app->request->params ( "workflows" );
		
		DB::beginTransaction ();
		$exists = DB::query ( 'SELECT count(*) AS count, MAX(id) AS id FROM workflow_groups WHERE user_id = ? AND name LIKE ?', $userId, $name );
		
		$id = 0;
		if ($exists [0]->count <= 0) {
			// INSERT
			DB::exec ( "INSERT INTO workflow_groups(user_id, name) VALUES (?, ?)", $userId, $name );
			$id = DB::getLastInsertedId ( "workflow_groups_id_seq" );
		} else {
			// UPDATE
			$id = $exists [0]->id;
			DB::exec ( "UPDATE workflow_groups SET changed = NOW() WHERE user_id = ? AND id = ?", $userId, $id );
		}
		
		// DELETE OLD ENTRIES AND ADD NEW ONES
		DB::exec ( "DELETE FROM workflows WHERE workflow_group_id = ?", $id );
		
		foreach ( $workflows as $workflow ) {
			DB::exec ( "INSERT INTO workflows(workflow_group_id, graph, name) VALUES (?, ?, ?)", $id, $workflow ["query"], $workflow ["name"] );
		}
		
		DB::commit ();
		
		echo '{"status": "OK"}';
	} );
} );

/**
 * Storage and retrieval of user defined scripts.
 */
$app->group ( '/scripts', function () use($app) {
	/**
	 * Get a list of all scripts.
	 */
	$app->get ( '/', function () use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			echo array ();
		}
		$userId = $app->request->authentication ["userId"];
		
		$scripts = DB::query ( 'SELECT id, name FROM scripts WHERE user_id = ? ORDER BY name ASC', $userId );
		
		$output = array ();
		foreach ( $scripts as $script ) {
			$output [] = array (
					"id" => $script->id,
					"name" => $script->name 
			);
		}
		
		echo json_encode ( $output );
	} );
	
	/**
	 * Get one scripts.
	 */
	$app->get ( '/:scriptId', function ($scriptId) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			echo new stdClass ();
		}
		$userId = $app->request->authentication ["userId"];
		
		$scripts = DB::query ( 'SELECT script, result_type FROM scripts WHERE user_id = ? AND id = ?', $userId, $scriptId );
		
		$output = new stdClass ();
		foreach ( $scripts as $script ) {
			$output->code = $script->script;
			$output->resultType = $script->result_type;
		}
		
		echo json_encode ( $output );
	} );
	
	/**
	 * Store a script.
	 */
	$app->post ( '/:name', function ($name) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$script = $app->request->params ( "script" );
		
		DB::beginTransaction ();
		$exists = DB::query ( 'SELECT count(*) AS count, MAX(id) AS id FROM scripts WHERE user_id = ? AND name LIKE ?', $userId, $name );
		
		if ($exists [0]->count <= 0) {
			// INSERT
			DB::exec ( "INSERT INTO scripts(user_id, name, script, result_type) VALUES (?, ?, ?, ?)", $userId, $name, $script["code"], $script["resultType"] );
		} else {
			// UPDATE
			$id = $exists [0]->id;
			DB::exec ( "UPDATE scripts SET script = ?, result_type = ? WHERE user_id = ? AND id = ?", $script["code"], $script["resultType"], $userId, $id );
		}
		
		DB::commit ();
		
		echo '{"status": "OK"}';
	} );
} );

$app->run ();
?>