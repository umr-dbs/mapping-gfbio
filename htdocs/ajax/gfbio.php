<?php
/*
 * Author - Rob Thomson <rob@marotori.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

session_start();
ob_start();

/* config settings */
//$base = "http://10.0.9.3:8080/gfbio-prototype/"; //set this to the url you want to scrape
$base = "http://dbsvm.mathematik.uni-marburg.de:9833/gfbio-prototype/"; //set this to the url you want to scrape


/* all system code happens below - you should not need to edit it! */

$base = rtrim($base, '/');

$parameters = str_replace($_SERVER['SCRIPT_NAME'].'/', '', $_SERVER['REQUEST_URI']);
$url = $base . '/' . $parameters;

$mydomain = ((isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on') ? 'https://' : 'http://') . $_SERVER["HTTP_HOST"].$_SERVER['SCRIPT_NAME'].'/';

//die($mydomain."\n".$url);
// Open the cURL session
$curlSession = curl_init();

curl_setopt ($curlSession, CURLOPT_URL, $url);
curl_setopt ($curlSession, CURLOPT_HEADER, 1);

$proxy = 'www-cache.mathematik.uni-marburg.de:3128';
if ($proxy)
	curl_setopt($curlSession, CURLOPT_PROXY, $proxy);

if($_SERVER['REQUEST_METHOD'] == 'POST'){
        curl_setopt ($curlSession, CURLOPT_POST, 1);
        curl_setopt ($curlSession, CURLOPT_POSTFIELDS, $_POST);
}

curl_setopt($curlSession, CURLOPT_RETURNTRANSFER,1);
curl_setopt($curlSession, CURLOPT_TIMEOUT,30);


//Send the request and store the result in an array
$response = curl_exec ($curlSession);

// Check that a connection was made
if (curl_error($curlSession)){
	// If it wasn't...
	print curl_error($curlSession);
} else {

	//clean duplicate header that seems to appear on fastcgi with output buffer on some servers!!
	$response = str_replace("HTTP/1.1 100 Continue\r\n\r\n","",$response);

	$ar = explode("\r\n\r\n", $response, 2);

	header('Content-Type: application/json');

	$header = $ar[0];
	$body = $ar[1];

	//handle headers - simply re-outputing them
	$header_ar = explode(chr(10),$header);
	foreach($header_ar as $k=>$v){
		if(!preg_match("/^Transfer-Encoding/",$v)){
			$v = str_replace($base,$mydomain,$v); //header rewrite if needed
			header(trim($v));
		}
	}

	//rewrite all hard coded urls to ensure the links still work!
	$body = str_replace($base,$mydomain,$body);

	print $body;

}

curl_close ($curlSession);
