#!/usr/bin/php
<?php

$date = date('r');

$expected_results = array('hash', 'valgrind');

echo <<<EOS
<html>
	<head><title>Testresults $date</title></head>
	<script type="text/javascript">
	function toggle(id) {
		var e = document.getElementsByClassName('collapse_'+id);
		for (var i=0;i<e.length;i++)
			e[i].style.display = e[i].style.display == 'none' ? '' : 'none';
	}
	</script>
	<body>
		<h1>Testresults $date</h1>
		<table border="1" cellspacing="0">

EOS;

define('ROOT', 'test/queries/');

$logs = array();
$dir = opendir(ROOT);
while ($file = readdir($dir)) {
	$path = ROOT . $file;
	if (!is_file($path))
		continue;
	$pi = pathinfo($path);
	if ($pi['extension'] != 'log')
		continue;
	$logs[] = $file;
}
closedir($dir);

sort($logs);

$collapse_id = 1;
foreach ($logs as $file) {
	$passed = array();
	$log = array();
	$lines = file(ROOT . $file);
	$current_subject = $current_result = false;
	foreach ($lines as $line) {
		if (preg_match('/(.*)_passed: (.*)/', $line, $matches)) {
			$current_subject = $matches[1];
			$current_result = $matches[2] == 'yes' ? true : false;
			$passed[$current_subject] = $current_result;
			$log[$current_subject] = '';
		}
		if (!$current_subject)
			continue;
		if ($passed[$current_subject])
			continue;
		$log[$current_subject] .= $line;
	}

	$all_passed = true;
	foreach($expected_results as $name) {
		if (isset($passed[$name]) && !$passed[$name])
			$all_passed = false;
	}
	echo '<tr'.($all_passed ? '' : ' onclick="toggle('.$collapse_id.');" style="cursor: pointer;"').'><td>'.$file.'</td>';
	foreach($expected_results as $name) {
		if (!isset($passed[$name]))
			$col = 'blue';
		else if ($passed[$name])
			$col = 'green';
		else
			$col = 'red';
		echo '<td style="background-color: ' . $col . '">'.$name.'</td>';
	}
	echo "</tr>\n";
	foreach($expected_results as $name) {
		if (!isset($passed[$name]) || $passed[$name])
			continue;
		echo '<tr class="collapse_'.$collapse_id.'" style="display: none"><td colspan="'.(count($expected_results)+1).'"><b>'.$name.'</b><br /><pre>'.$log[$name].'</pre></td></tr>'."\n";
	}

	$collapse_id++;
}


echo <<<EOS
</table></body></html>

EOS;
