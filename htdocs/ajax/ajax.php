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

	$sessiontoken = md5(mt_rand()); // TODO: not much entropy..

	// guest login..
	if ($username == 'guest' && $password == 'guest') {
		respond(array(
			'userid' => -1,
			'session' => $sessiontoken,
			'ui' => 'previews'
		));
	}


	// TODO: lower() prevents using the index. Postgres has column type "citext", but is that what we want?
	$rows = DB::query('SELECT id, ui FROM users WHERE lower(name) = lower(?) AND password_unhashed = ?', $username, $password);
	if (count($rows) < 1)
		respond_fail('User or password wrong');
	$id = $rows[0]->id;

	DB::exec('UPDATE users SET session = ? WHERE id = ?', $sessiontoken, $id);
	respond(array(
		'userid' => $id,
		'session' => $sessiontoken,
		'ui' => $rows[0]->ui
	));
}


// Check login
$userid = $_GET['userid'];
$sessiontoken = $_GET['session'];

if ($userid == -1) {
	// Guest login, it's ok..
}
else {
	$userrows = DB::query('SELECT * FROM users WHERE id = ? AND session = ?', $userid, $sessiontoken);
	if (count($userrows) < 1) {
		respond_fail('Session invalid');
	}
}


// Do whatever is required
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

		$source = json_decode(file_get_contents($path));
		if ($source)
			$result[$name] = $source;
	}

	respond(array(
		'sourcelist' => $result
	));
}


if ($action == 'examplequerylist.get') {
	$result = json_decode(<<<EOS
{
"OSM": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "OSM: primary roads of spain",

	"query": {
		"type": "osmgeometrysource",
		"params": {
		},
		"sources": []
	}
},
"geometrytest": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Geometry test",

	"query": {
		"type": "testgeometrysource",
		"params": {
		},
		"sources": []
	}
},
"reflectance": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "_MSat2 #6 Radiance",

	"query": {
		"type": "msatradiance",
		"params": {
		},
		"sources": [{
			"type": "source",
			"params": {
				"sourcepath": "datasources/msat2.json",
				"channel": 6
			}
		}]
	}
},
"temperature": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "_MSat2 #6 Temperature",

	"query": {
		"type": "msattemperature",
		"params": {
		},
		"sources": [{
			"type": "source",
			"params": {
				"sourcepath": "datasources/msat2.json",
				"channel": 6
			}
		}]
	}
},
"clouds": {
	"colorizer": "grey",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Fake cloud-detection algorithm",

	"query": {
		"type": "projection",
		"params": {
			"src_epsg": 62866,
			"dest_epsg": 3857
		},
		"sources": [{
			"type": "expression",
			"params": {
				"expression": "value < 300 ? 1 : 0"
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 6
				}
			}]
		}]
	}
},
"gfbio": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Puma sightings (gfbio-server)",

	"query": {
		"type": "points2raster",
		"params": {
		},
		"sources": [{
			"type": "gfbiopointsource",
			"params": {
				"datasource": "GBIF",
				"query": "{\"globalAttributes\":{\"speciesName\":\"Puma concolor\"},\"localAttributes\":{}}"
			}
		}]
	}
},
"kangaroos": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Kangaroo sightings (gbif)",

	"query": {
		"type": "points2raster",
		"params": {
		},
		"sources": [{
			"type": "pgpointsource",
			"params": {
				"connection": "host = '10.0.9.3' dbname = 'postgres' user = 'postgres' password = 'test'",
				"query": "x, y FROM (SELECT ST_X(t.geom) x, ST_Y(t.geom) y FROM (SELECT ST_TRANSFORM(location, 3857) geom FROM public.gbif_taxon_to_name JOIN public.gbif_lite_time ON (gbif_taxon_to_name.taxon = gbif_lite_time.taxon) WHERE name = 'Macropus rufus') t) t2"
			}
		}]
	}
},
"msat0_1": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Average of msat channels 0 and 1",

	"query": {
		"type": "projection",
		"params": {
			"src_epsg": 62866,
			"dest_epsg": 3857
		},
		"sources": [{
			"type": "add",
			"params": {
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 0
				}
			}, {
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 1
				}
			}]
		}]
	}

},
"msat0_clexpression": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "A test-expression inverting a meteosat-layer",

	"query": {
		"type": "projection",
		"params": {
			"src_epsg": 62866,
			"dest_epsg": 3857
		},
		"sources": [{
			"type": "expression",
			"params": {
				"expression": "1024-value"
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 0
				}
			}]
		}]
	}
},
"msat0_edge": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Edge detection to test the matrix operator",

	"query": {
		"type": "matrix",
		"params": {
			"matrix_size": 3,
			"matrix": [-1,-1,-1,-1,8,-1,-1,-1,-1]
		},
		"sources": [{
			"type": "projection",
			"params": {
				"src_epsg": 62866,
				"dest_epsg": 3857
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 0
				}
			}]
		}]
	}
},
"msat0_invertcl": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "A random opencl script on msat",

	"query": {
		"type": "projection",
		"params": {
			"src_epsg": 62866,
			"dest_epsg": 3857
		},
		"sources": [{
			"type": "opencl",
			"params": {
				"source": "test.cl",
				"kernel": "testKernel"
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 0
				}
			}]
		}]
	}
},
"msat0_invert": {
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "A specialized operator inverting a meteosat-layer",

	"query": {
		"type": "projection",
		"params": {
			"src_epsg": 62866,
			"dest_epsg": 3857
		},
		"sources": [{
			"type": "negate",
			"params": {
			},
			"sources": [{
				"type": "source",
				"params": {
					"sourcepath": "datasources/msat2.json",
					"channel": 0
				}
			}]
		}]
	}
},
"pumas": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Puma sightings from PostGIS (as Points)",

	"query": {
		"type": "pgpointsource",
		"params": {
			"query": "x, y FROM locations"
		}
	}
},
"pumas2": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Puma sightings from PostGIS (rastered)",

	"query": {
		"type": "points2raster",
		"params": {
		},
		"sources": [{
			"type": "pgpointsource",
			"params": {
				"query": "x, y FROM locations"
			}
		}]
	}
},
"pumasunderclouds": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Pumas that are currently under clouds",

	"query": {
		"type": "filterpointsbyraster",
		"params": {
		},
		"sources": [{
			"type": "pgpointsource",
			"params": {
				"query": "x, y FROM locations"
			}
		},{
			"type": "projection",
			"params": {
				"src_epsg": 62866,
				"dest_epsg": 3857
			},
			"sources": [{
				"type": "expression",
				"params": {
					"expression": "value < 300 ? 1 : 0"
				},
				"sources": [{
					"type": "source",
					"params": {
						"sourcepath": "datasources/msat2.json",
						"channel": 6
					}
				}]
			}]
		}]
	}
},
"rats2": {
	"colorizer": "hsv",
	"starttime": 42,
	"endtime": 42,
	"timeinterval": 1,

	"name": "Points of Rats from gbif",

	"query": {
		"type": "points2raster",
		"params": {
		},
		"sources": [{
			"type": "pgpointsource",
			"params": {
				"connection": "host = '10.0.9.3' dbname = 'postgres' user = 'postgres' password = 'test'",
				"query": "x, y FROM (SELECT ST_X(t.geom) x, ST_Y(t.geom) y FROM (SELECT ST_TRANSFORM(location, 3857) geom FROM public.gbif_taxon_to_name JOIN public.gbif_lite_time ON (gbif_taxon_to_name.taxon = gbif_lite_time.taxon) WHERE name = 'Rattus rattus') t) t2"
			}
		}]
	}
}
}
EOS
	);
	respond(array(
		'querylist' => $result
	));
}
