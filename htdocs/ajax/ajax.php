<?php

require_once('database.php');


function respond($object) {
	header('Content-type: application/json');
	if (!isset($object['result']))
		$object['result'] = true;
	die(json_encode($object));
}

function respond_fail($error) {
	respond(array(
		'result' => $error
	));
}



$action = $_GET['action'];

if ($action == 'login.login') {
	$username = trim($_GET['username']);
	$password = trim($_GET['password']);

	// TODO: lower() prevents using the index. Postgres has column type "citext", but is that what we want?
	$rows = DB::query('SELECT id FROM users WHERE lower(name) = lower(?) AND password_unhashed = ?', $username, $password);
	if (count($rows) < 1)
		respond_fail('User or password wrong');
	$id = $rows[0]->id;

	$sessiontoken = md5(mt_rand()); // TODO: not much entropy..
	DB::exec('UPDATE users SET session = ? WHERE id = ?', $sessiontoken, $id);
	respond(array(
		'userid' => $id,
		'session' => $sessiontoken
	));
}


// Check login
/*
$userid = $_GET['userid'];
$sessiontoken = $_GET['session'];

$userrows = DB::query('SELECT * FROM users WHERE id = ? AND session = ?', $userid, $sessiontoken);
if (count($userrows) < 1) {
	respond_fail('Session invalid');
}
*/


if ($action == 'sourcelist.get') {
	$sourcespath = __DIR__ . '/../cgi-bin/datasources/';
	// read all sources

	$dir = opendir($sourcespath);
	if (!$dir)
		respond_fail('Server could not read datasources.');

	$files = array();
	while (($file = readdir($dir)) !== false) {
		$path = $sourcespath . $file;
		if (!is_file($path))
			continue;
		$pathinfo = pathinfo($path);
		if (!isset($pathinfo['extension']) || $pathinfo['extension'] != 'json')
			continue;

		$files[] = $path;
	}
	closedir($dir);

	$result = array();
	foreach($files as $path) {
		$pathinfo = pathinfo($path);
		$name = $pathinfo['filename'];

		$result[$name] = json_decode(file_get_contents($path));
	}

	respond(array(
		'sourcelist' => $result
	));
}


if ($action == 'examplequerylist.get') {
	$sourcespath = __DIR__ . '/../../queries/';
	// read all sources

	$dir = opendir($sourcespath);
	if (!$dir)
		respond_fail('Server could not read queries.');

	$files = array();
	while (($file = readdir($dir)) !== false) {
		$path = $sourcespath . $file;
		if (!is_file($path))
			continue;
		$pathinfo = pathinfo($path);
		if (!isset($pathinfo['extension']) || $pathinfo['extension'] != 'json')
			continue;

		$files[] = $path;
	}
	closedir($dir);

	$result = array();
	foreach($files as $path) {
		$pathinfo = pathinfo($path);
		$name = $pathinfo['filename'];

		$query = json_decode(file_get_contents($path));
		if ($query)
			$result[$name] = $query;
	}

	respond(array(
		'querylist' => $result
	));
}
