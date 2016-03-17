#!/usr/bin/php
<?php

// These are test failures, e.g. the test had the wrong result
$failure_passes = array('semantic', 'hash');
// These are test errors, e.g. the test crashed or did not run correctly
$error_passes = array('returncode', 'elapsedtime', 'sanitizer');
// These passes are reported in the HTML output
$reported_passes = array('semantic', 'hash', 'sanitizer');
define('ROOT', 'test/systemtests/queries/');


if ($argc != 3)
	die("Needs two parameters: the file for HTML output and the file for JUnit XML output\n");

$html_file = $argv[1];
$xml_file = $argv[2];

$timestamp = time();
$datehuman = date('r', $timestamp);
$dateiso = date('c', $timestamp);


// Output as a human-readable HTML page, including logs.
$html_out = <<<EOS
<html>
	<head><title>Testresults $datehuman</title>
	<script type="text/javascript">
	function toggle(id) {
		var e = document.getElementsByClassName('collapse_'+id);
		for (var i=0;i<e.length;i++)
			e[i].style.display = e[i].style.display == 'none' ? '' : 'none';
	}
	</script>
	<style>
   	thead td {
		font-weight: bold;
	}
	td {
		text-align: center;
		padding: 1px 5px;
		margin: 0;
	}
	td:first-child {
		text-align: left;
	}
    </style>
    </head>
	<body>
		<h1>Testresults $datehuman</h1>
		<table border="1" cellspacing="0">
		<thead><tr><td>Name</td><td style="min-width: 70px;">Time</td>
EOS;

foreach($reported_passes as $pass)
	$html_out .= '<td style="min-width: 100px;">' . $pass . '</td>';
$html_out .= '<td style="min-width: 50px;">Exit code</td></tr></thead>' . "\n";

// Output in JUnit's XML format, documented for example here:
// https://github.com/windyroad/JUnit-Schema/blob/master/JUnit.xsd
$xml_out = new DOMDocument('1.0', 'UTF-8');
$xml_out_testsuites = $xml_out->createElement('testsuites');
$xml_out_testsuites->setAttribute('timestamp', $dateiso);
$xml_out_testsuites->setAttribute('name', 'AllSystemTests');
$xml_out->appendChild($xml_out_testsuites);

$xml_out_testsuite = $xml_out->createElement('testsuite');
$xml_out_testsuite->setAttribute('name', 'Systemtests');
$xml_out_testsuites->appendChild($xml_out_testsuite);

// Find all log files
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


// Parse all logs and generate output
$tests_passed = 0;
$tests_total = 0;
$tests_totaltime = 0;

$collapse_id = 0;
foreach ($logs as $file) {
	$collapse_id++;

	$log = file_get_contents(ROOT . $file);
	$logname = str_replace('.log', '', $file);

	// We need to manually search the log for indications of successes or failures
	$elapsedtime = -1;
	$returncode = -1;
	$passed = array();

	// By default, all passes are FAIL unless the log says otherwise
	foreach($failure_passes as $name)
		$passed[$name] = false;
	foreach($error_passes as $name)
		$passed[$name] = false;

	// These are considered PASS unless the log says otherwise.
	$passed['sanitizer'] = true;

	// Find the returncode and time
	if (preg_match('/^TESTCASE_ELAPSED_TIME: ([.0-9]+)$/m', $log, $matches)) {
		$elapsedtime = doubleval($matches[1]);
		$passed['elapsedtime'] = true;
	}
	if (preg_match('/^TESTCASE_RETURN_CODE: ([0-9]+)$/m', $log, $matches)) {
		$returncode = intval($matches[1]);
		if ($returncode == 0)
			$passed['returncode'] = true;
	}

	// Find indications of PASS or FAIL in the log
	foreach($reported_passes as $pass) {
		if (strpos($log, "PASSED: $pass\n") !== FALSE)
			$passed[$pass] = true;
		if (strpos($log, "FAILED: $pass\n") !== FALSE)
			$passed[$pass] = false;
	}

	// Watch for output from the sanitizers
	if (strpos($log, 'AddressSanitizer') !== FALSE || strpos($log, 'LeakSanitizer') !== FALSE || strpos($log, ': runtime error: ') !== FALSE)
		$passed['sanitizer'] = false;

	// Determine if the whole test is considered PASS or not
	$all_passed = true;
	foreach($passed as $name => $success) {
		if (!$success)
			$all_passed = false;
	}

	$tests_total++;
	if ($all_passed)
		$tests_passed++;
	$tests_totaltime += max(0, $elapsedtime);

	// Generate HTML output
	$html_out .= '<tr'.($all_passed ? '' : ' onclick="toggle('.$collapse_id.');" style="cursor: pointer;"').'><td>'.$logname.'</td>';

	$html_out .= '<td style="text-align: right;">' . number_format($elapsedtime, 3, '.', ' ') . 's</td>';

	foreach($reported_passes as $pass) {
		if (!isset($passed[$pass]))
			$col = 'blue';
		else if ($passed[$pass])
			$col = 'green';
		else
			$col = 'red';
		$html_out .= '<td style="background-color: ' . $col . '">'.$pass.'</td>';
	}
	$html_out .= '<td style="background-color: ' . ($returncode == 0 ? 'green' : 'red') . '">' . $returncode . '</td>';

	$html_out .= "</tr>\n";
	if (!$all_passed) {
		$html_out .= '<tr class="collapse_'.$collapse_id.'" style="display: none"><td colspan="'.(count($reported_passes)+3).'"><b>Execution log</b><br /><pre>'.htmlspecialchars($log, ENT_NOQUOTES).'</pre></td></tr>'."\n";
	}

	// Generate XML output
	$xml_testcase = $xml_out->createElement('testcase');
	$xml_testcase->setAttribute('name', $logname);
	$xml_testcase->setAttribute('classname', 'systemtests.'.$logname);
	$xml_testcase->setAttribute('status', 'run');
	if ($elapsedtime >= 0)
		$xml_testcase->setAttribute('time', $elapsedtime);

	if (!$all_passed) {
		$failed = array();
		$is_error = false;
		foreach($passed as $name => $success) {
			if (!$success) {
				if (in_array($name, $error_passes))
					$is_error = true;
				$failed[] = $name;
			}
		}
		$failure = $xml_out->createElement($is_error ? 'error' : 'failure', $log);
		$failure->setAttribute('message', 'Failed: ' . implode(', ', $failed));
		$xml_testcase->appendChild($failure);
	}

	$xml_out_testsuite->appendChild($xml_testcase);
}

// Finish HTML output
$html_out .= '</table></body></html>';
file_put_contents($html_file, $html_out);

// Finish XML output
// <testsuites tests="163" failures="0" disabled="0" errors="0" timestamp="2016-03-17T13:30:04" time="0.419" name="AllTests">
// <testsuite name="CSVParser" tests="12" failures="0" disabled="0" errors="0" time="0">
function xml_set_aggregates($node, $passed, $total, $time) {
	$node->setAttribute('tests', $total);
	$node->setAttribute('failures', $total-$passed);
	$node->setAttribute('disabled', 0);
	$node->setAttribute('errors', 0);
	$node->setAttribute('time', number_format($time, 3, '.', ''));
}
xml_set_aggregates($xml_out_testsuite, $tests_total, $tests_passed, $tests_totaltime);
xml_set_aggregates($xml_out_testsuites, $tests_total, $tests_passed, $tests_totaltime);
$xml_out->formatOutput = true;
$xml_out->save($xml_file);
