<?php

$file = fopen('places_dump_DE.geojson', 'r');

echo "X,Y,Name,PLZ\n";

$linenr = 0;
while (($line = fgets($file)) !== false) {
	$linenr++;
	$line = trim($line);
	if ($line == '')
		continue;
	$obj = json_decode($line);
	if (!$obj)
		die("Line $linenr is not valid json\n");

	if ($obj->type != 'Feature')
		die("Object in line $linenr is not a Feature\n");
	if ($obj->geometry->type != 'Point')
		die("Object in line $linenr has a geometry that is not a Point\n");
	$coords = $obj->geometry->coordinates;
	$x = $coords[0];
	$y = $coords[1];
	$props = $obj->properties;
	if (!isset($props->postcode))
		continue;
	$name = mb_substr($props->name, 0, 10);
	$plz = (int) $props->postcode;
	if (strpos($name, '"') !== false || strpos($name, "\n") !== false)
		continue;
	if ($plz <= 0 || $plz > 99999)
		continue;
	printf("%.5f,%.5f,\"%s\",%d\n", $x,$y,$name,$plz);
}

fclose($file);
