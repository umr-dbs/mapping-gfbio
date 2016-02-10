#!/usr/bin/php
<?php

$date = date('r');

$expected_passes = array('semantic', 'hash', 'addrsan');

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

define('ROOT', 'test/systemtests/queries/');

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
	$all_passed = true;
	$log = '';
	$lines = file(ROOT . $file);

	if (trim($lines[0]) == 'PASSED: all') {
		foreach($expected_passes as $pass)
			$passed[$pass] = true;
		$all_passed = true;
	}
	else {
		$log = implode($lines);

		// If something failed, we need to manually search the log for indications of partial successes or failures
		if (strpos($log, "\nPASSED: semantic\n") !== FALSE)
			$passed['semantic'] = true;
		if (strpos($log, "\nFAILED: semantic\n") !== FALSE)
			$passed['semantic'] = false;

		if (strpos($log, "\nPASSED: hash\n") !== FALSE)
			$passed['hash'] = true;
		if (strpos($log, "\nFAILED: hash\n") !== FALSE)
			$passed['hash'] = false;

		if (strpos($log, 'AddressSanitizer') === FALSE && strpos($log, 'LeakSanitizer') === FALSE)
			$passed['addrsan'] = true;
		else
			$passed['addrsan'] = false;

		foreach($expected_passes as $pass) {
			if (!isset($passed[$pass]) || !$passed[$pass])
				$all_passed = false;
		}
	}
	echo '<tr'.($all_passed ? '' : ' onclick="toggle('.$collapse_id.');" style="cursor: pointer;"').'><td>'.$file.'</td>';

	foreach($expected_passes as $pass) {
		if (!isset($passed[$pass]))
			$col = 'blue';
		else if ($passed[$pass])
			$col = 'green';
		else
			$col = 'red';
		echo '<td style="background-color: ' . $col . '">'.$pass.'</td>';
	}
	echo "</tr>\n";
	if (!$all_passed) {
		echo '<tr class="collapse_'.$collapse_id.'" style="display: none"><td colspan="'.(count($expected_passes)+1).'"><b>Execution log</b><br /><pre>'.$log.'</pre></td></tr>'."\n";
	}

	$collapse_id++;
}


echo <<<EOS
</table></body></html>

EOS;
