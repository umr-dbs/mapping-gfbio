<?php

define('SOURCEPATH', __DIR__.'/../src/');

$type = strtolower($_GET['type']);
$project = strtolower($_GET['project']);

if ($project != 'gfbio' && $project != 'idessa')
	die('Unknown project');

if ($type == 'css') {
	header("content-type: text/css");
}
else if ($type == 'js') {
	header("content-type: application/javascript");
	//echo "\"use strict\";\n\n";
}
else if ($type == 'soy') {
	define('SOYCOMPILEDFILE', __DIR__.'/compiled.tmp.soy.js');
	define('JARFILE', __DIR__.'/SoyToJsSrcCompiler.jar');
	header("content-type: application/javascript");
}
else
	die('Unknown type');


/**
 * Returns the modification time of a file, or 0 if file doesn't exist
 */
function mtime($path) {
	if (!file_exists($path))
		return 0;
	$s = stat($path);
	return (int) $s['mtime'];
}

/**
 * Returns an array of all files with a given extension in a given directory
 */
function getFiles($directorypath, $extension, $calculate_mtime = false) {
	$latest_mtime = 0;

	$dir = opendir($directorypath);
	if (!$dir)
		abort("Could not open directory ".$directorypath);

	$files = array();
	while (($file = readdir($dir)) !== false) {
		$filepath = $directorypath . $file;
		if (!is_file($filepath))
			continue;
		$pathinfo = pathinfo($filepath);
		if (!isset($pathinfo['extension']) || $pathinfo['extension'] != $extension)
			continue;

		$files[] = $filepath;
		if ($calculate_mtime)
			$latest_mtime = max($latest_mtime, mtime($path));
	}
	closedir($dir);
	sort($files);
	if ($calculate_mtime)
		return array('files' => $files, 'mtime' => $latest_mtime);
	return $files;
}

function outputFiles($directorypath, $extension) {
	$files = getFiles($directorypath, $extension);
	foreach($files as $path) {
		readfile($path);
		echo "\n";
	}
}


if ($type == 'css' || $type == 'js') {
	outputFiles(SOURCEPATH.'external/', $type);
	outputFiles(SOURCEPATH.$project.'/', $type);
	outputFiles(SOURCEPATH, $type);
}
else if ($type == 'soy') {
	$target_time = mtime(SOYCOMPILEDFILE);

	$sources = array();
	$most_recent_source_time = 0;

	$res1 = getFiles(SOURCEPATH, 'soy', true);
	$res2 = getFiles(SOURCEPATH.$project.'/', 'soy', true);

	$latest_mtime = max($res1['mtime'], $res2['mtime']);
	$sources = array_merge($res1['files'], $res2['files']);

	if (count($sources) == 0)
		abort('No .soy files found');

	if ($target_time == 0 || $target_time <= $latest_mtime) {
		// need to compile

		$command = 'java 2>&1 -jar '.JARFILE.' --outputPathFormat '.SOYCOMPILEDFILE.' --srcs '.implode(',', $sources);
		$result = 0;
		$output = array();
		exec($command, $output, $result);

		if ($result != 0) {
			abort($command."\n\nResult: $result\n\n".implode("\n", $output));
		}

		// java -jar SoyToJsSrcCompiler.jar --outputPathFormat simple.js --srcs simple.soy
	}

	readfile(SOYCOMPILEDFILE);
}
