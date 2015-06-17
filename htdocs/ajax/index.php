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
 * Storage and retrieval of projects.
 */
$app->group ( '/projects', function () use($app) {
	/**
	 * Get a list of all projects.
	 */
	$app->get ( '/', function () use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			$app->response->write ( new stdClass () );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$projects = DB::query ( "SELECT id, name, valid_from FROM projects WHERE user_id = ? AND valid_to = 'infinity'", $userId );
		
		$output = array ();
		foreach ( $projects as $project ) {
			$output [$project->id] = array (
					"name" => $project->name,
					"changed" => $project->valid_from 
			);
		}
		
		$app->response->write ( json_encode ( ( object ) $output ) );
	} );
	
	/**
	 * Get versions of a project.
	 */
	$app->get ( '/:projectId/versions', function ($projectId) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			$app->response->write ( array () );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$projectVersions = DB::query ( "SELECT id, name, valid_from FROM projects WHERE user_id = ? AND id = ? ORDER BY valid_from DESC", $userId, $projectId );
		
		$output = array ();
		foreach ( $projectVersions as $version ) {
			$output [] = array (
					"id" => $version->id,
					"name" => $version->name,
					"changed" => $version->valid_from 
			);
		}
		
		$app->response->write ( json_encode ( $output ) );
	} );
	
	/**
	 * Get a specific project.
	 */
	$app->get ( '/:projectId(/version/:timestamp)', function ($projectId, $timestamp = 'infinity') use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			$app->response->write ( array () );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		if ($timestamp == "infinity") {
			$workflows = DB::query ( "SELECT w.name, w.graph FROM workflows w JOIN projects p ON(p.id = w.project_id) WHERE p.user_id = ? AND p.id = ? AND p.valid_to = 'infinity' AND w.valid_to = 'infinity'", $userId, $projectId );
		} else {
			$workflows = DB::query ( "SELECT w.name, w.graph FROM workflows w JOIN projects p ON(p.id = w.project_id) WHERE p.user_id = ? AND p.id = ? AND (p.valid_from <= ? AND ? < p.valid_to) AND (w.valid_from <= ? AND ? < w.valid_to)", $userId, $projectId, $timestamp, $timestamp, $timestamp, $timestamp );
		}
		
		$output = array ();
		foreach ( $workflows as $workflow ) {
			$output [] = array (
					"name" => $workflow->name,
					"query" => $workflow->graph 
			);
		}
		
		$app->response->write ( json_encode ( $output ) );
	} );
	
	/**
	 * Store a project.
	 */
	$app->post ( '/:name', function ($name) use($app) {
		if (! $app->request->authentication ["loggedIn"]) {
			$app->halt ( 403 );
		}
		$userId = $app->request->authentication ["userId"];
		
		DB::beginTransaction ();
		
		// GET A PROJECT ID
		$projectId = null;
		$exists = DB::query ( "SELECT id, valid_to FROM projects WHERE user_id = ? AND name LIKE ? AND valid_to >= ALL (SELECT valid_to FROM projects WHERE user_id = ? AND name LIKE ?)", $userId, $name, $userId, $name );
		if (count ( $exists ) <= 0) {
			// INSERT
			DB::exec ( "INSERT INTO projects(user_id, name) VALUES (?, ?)", $userId, $name );
			$projectId = DB::getLastInsertedId ( "project_id_seq" );
		} else {
			// UPDATE
			$projectId = $exists [0]->id;
			if ($exists [0]->valid_to == "infinity") {
				// close the current group
				DB::exec ( "UPDATE projects SET valid_to = NOW() WHERE user_id = ? AND id = ? AND valid_to = 'infinity'", $userId, $projectId );
			}
			DB::exec ( "INSERT INTO projects(id, user_id, name) VALUES (?, ?, ?)", $projectId, $userId, $name );
		}
		
		// PROCESS WORKFLOWS
		$workflows = $app->request->params ( "workflows" );
		
		// -- THESE WORKFLOWS CAN STAY UNCHANGED
		$existingWorkflows = DB::query ( "SELECT id, name, graph FROM workflows WHERE project_id = ? AND valid_to = 'infinity'", $projectId );
		foreach ( $existingWorkflows as $existingWorkflow ) {
			$stillExists = false;
			
			foreach ( $workflows as $workflowKey => $workflow ) {
				if ($existingWorkflow->name == $workflow ["name"] && $existingWorkflow->graph == $workflow ["query"]) {
					$stillExists = true;
					// remove unchanged workflows
					unset ( $workflows [$workflowKey] );
					break;
				}
			}
			
			if (! $stillExists) {
				// close the old ones
				DB::exec ( "UPDATE workflows SET valid_to = NOW() WHERE project_id = ? AND id = ? AND valid_to = 'infinity'", $projectId, $existingWorkflow->id );
			}
		}
		
		// INSERT NEW WORKFLOWS
		$workflowNames = array ();
		foreach ( $workflows as $workflow ) {
			$workflowNames [] = $workflow ["name"];
		}
		$existingWorkflows = DB::query ( "SELECT id, name FROM workflows WHERE project_id = ? AND name = ANY(?)", $projectId, "{" . implode ( ",", $workflowNames ) . "}" );
		
		$updatables = array ();
		foreach ( $existingWorkflows as $existingWorkflow ) {
			$updatables [$existingWorkflow->name] = $existingWorkflow->id;
		}
		
		foreach ( $workflows as $workflow ) {
			if (isset ( $updatables [$workflow ["name"]] )) {
				// match to existing workflow (id + name)
				$id = $updatables [$workflow ["name"]];
				DB::exec ( "INSERT INTO workflows(id, project_id, graph, name) VALUES (?, ?, ?, ?)", $id, $projectId, $workflow ["query"], $workflow ["name"] );
			} else {
				DB::exec ( "INSERT INTO workflows(project_id, graph, name) VALUES (?, ?, ?)", $projectId, $workflow ["query"], $workflow ["name"] );
			}
		}
		
		DB::commit ();
		
		$app->response->setStatus ( 204 );
	} );
	
	/**
	 * Delete a project.
	 */
	$app->delete ( '/:projectId', function ($projectId) use($app) {
		if (! $app->request->authentication ["loggedIn"]) {
			$app->halt ( 403 );
		}
		$userId = $app->request->authentication ["userId"];
		
		DB::beginTransaction ();
		
		DB::exec ( "UPDATE projects SET valid_to = NOW() WHERE user_id = ? AND id = ? AND valid_to = 'infinity'", $userId, $projectId );
		DB::exec ( "UPDATE workflows SET valid_to = NOW() WHERE project_id = ? AND valid_to = 'infinity'", $projectId );
		
		DB::commit ();
		
		$app->response->setStatus ( 204 );
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
			$app->response->write ( json_encode ( array () ) );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$scripts = DB::query ( "SELECT id, name, valid_from FROM scripts WHERE user_id = ? AND valid_to = 'infinity' ORDER BY name ASC", $userId );
		
		$output = array ();
		foreach ( $scripts as $script ) {
			$output [] = array (
					"id" => $script->id,
					"name" => $script->name,
					"changed" => $script->valid_from 
			);
		}
		
		$app->response->write ( json_encode ( $output ) );
	} );
	
	/**
	 * Get versions of a script.
	 */
	$app->get ( '/:scriptId/versions', function ($scriptId) use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			$app->response->write ( array () );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		$scriptVersions = DB::query ( "SELECT id, name, valid_from FROM scripts WHERE user_id = ? AND id = ? ORDER BY valid_from DESC", $userId, $scriptId );
		
		$output = array ();
		foreach ( $scriptVersions as $version ) {
			$output [] = array (
					"id" => $version->id,
					"name" => $version->name,
					"changed" => $version->valid_from 
			);
		}
		
		$app->response->write ( json_encode ( $output ) );
	} );
	
	/**
	 * Get one script.
	 */
	$app->get ( '/:scriptId(/version/:timestamp)', function ($scriptId, $timestamp = 'infinity') use($app) {
		$app->response ()->header ( 'Content-Type', 'application/json' );
		
		if (! $app->request->authentication ["loggedIn"]) {
			$app->response->write ( json_encode ( stdClass () ) );
			return;
		}
		$userId = $app->request->authentication ["userId"];
		
		if ($timestamp == "infinity") {
			$scripts = DB::query ( "SELECT script, result_type FROM scripts WHERE user_id = ? AND id = ? AND valid_to = 'infinity'", $userId, $scriptId );
		} else {
			$scripts = DB::query ( "SELECT script, result_type FROM scripts WHERE user_id = ? AND id = ? AND (valid_from <= ? AND ? < valid_to)", $userId, $scriptId, $timestamp, $timestamp );
		}
		
		$output = new stdClass ();
		foreach ( $scripts as $script ) {
			$output->code = $script->script;
			$output->resultType = $script->result_type;
		}
		
		$app->response->write ( json_encode ( $output ) );
	} );
	
	/**
	 * Store a script.
	 */
	$app->post ( '/:name', function ($name) use($app) {
		if (! $app->request->authentication ["loggedIn"]) {
			$app->halt ( 403 );
		}
		$userId = $app->request->authentication ["userId"];
		
		$script = $app->request->params ( "script" );
		
		DB::beginTransaction ();
		$exists = DB::query ( "SELECT id, valid_to FROM scripts WHERE user_id = ? AND name LIKE ? AND valid_to >= ALL(SELECT valid_to FROM scripts WHERE user_id = ? AND name LIKE ?)", $userId, $name, $userId, $name );
		
		if (count($exists) <= 0) {
			// INSERT
			DB::exec ( "INSERT INTO scripts(user_id, name, script, result_type) VALUES (?, ?, ?, ?)", $userId, $name, $script ["code"], $script ["resultType"] );
		} else {
			// UPDATE
			$id = $exists [0]->id;
			if ($exists [0]->valid_to == 'infinity') {
				// close old entry
				DB::exec ( "UPDATE scripts SET valid_to = NOW() WHERE user_id = ? AND id = ? AND valid_to = 'infinity'", $userId, $id );
			}
			DB::exec ( "INSERT INTO scripts(id, user_id, name, script, result_type) VALUES (?, ?, ?, ?, ?)", $id, $userId, $name, $script ["code"], $script ["resultType"] );
		}
		
		DB::commit ();
		
		$app->response->setStatus ( 204 );
	} );
	
	/**
	 * Delete a script.
	 */
	$app->delete ( '/:scriptId', function ($scriptId) use($app) {
		if (! $app->request->authentication ["loggedIn"]) {
			$app->halt ( 403 );
		}
		$userId = $app->request->authentication ["userId"];
		
		DB::beginTransaction ();
		
		DB::exec ( "UPDATE scripts SET valid_to = NOW() WHERE user_id = ? AND id = ? AND valid_to = 'infinity'", $userId, $scriptId );
		
		DB::commit ();
		
		$app->response->setStatus ( 204 );
	} );
} );

/**
 * GFBio related WS
 */
$app->group ( '/gfbio', function () use($app) {

	/**
	 * get Baskets for liferay ID
	 */
	$app->get ( '/baskets/:liferayId', function ($liferayId) use($app) {
		//TODO: get user's liferayId from database instead of taking it as an parameter
		$curl = curl_init();		
		
		curl_setopt($curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
		curl_setopt($curl, CURLOPT_USERPWD, "vat_system@outlook.de:RL6z1Q1");
		
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
		curl_setopt($curl, CURLOPT_PROXY, "www-cache.mathematik.uni-marburg.de:3128");
		curl_setopt($curl, CURLOPT_FAILONERROR, true);
		
		curl_setopt($curl, CURLOPT_URL, "http://gfbio-dev1.inf-bb.uni-jena.de:8080/api/jsonws/GFBioProject-portlet.basket/get-baskets-by-user-id");
		curl_setopt($curl, CURLOPT_POST, 1);
		curl_setopt($curl, CURLOPT_POSTFIELDS, "userId=".$liferayId);
		
		$curlResult= curl_exec($curl);
		
		if($curlResult === false){
			$app->response->write( 'Curl-Fehler: ' . curl_error($curl));
		}
		else {
			$baskets = array();
			$liferayBaskets = json_decode($curlResult);
			foreach ($liferayBaskets as $liferayBasket){
				$basketJSON = json_decode($liferayBasket->basketJSON);			
				$basket = array("query" => json_decode($liferayBasket->queryJSON)->query->function_score->query->filtered->query->simple_query_string->query,
						"datetime" => gmdate("Y-m-d\TH:i:s\Z", $liferayBasket->lastModifiedDate/1000), 
						"results" => array());
				
				foreach ($basketJSON->selected as $basketEntryJSON){
					$basketEntry = array("title" => $basketEntryJSON->title,
							"authors" => $basketEntryJSON->authors,
							"dataCenter" => $basketEntryJSON->dataCenter,
							"metadataLink" => $basketEntryJSON->metadataLink);
					
					if(strpos($basketEntryJSON->metadataLink, "doi.pangaea.de")){						
						$basketEntry["type"] = "pangaea";
						$offset = strpos($basketEntryJSON->metadataLink, "doi.pangaea.de/") + strlen("doi.pangaea.de/");
						$basketEntry["doi"] = substr($basketEntryJSON->metadataLink, $offset);
					} else {
						$basketEntry["type"] = "abcd";
					}
					array_push($basket["results"], $basketEntry);
				}				
				
				array_push($baskets, $basket);
			}
			
			$app->response->write ( json_encode ( $baskets ) );			
		}
		
		curl_close($curl);		
	});
});
	
$app->run ();
?>