/*
 * exp_workflows.h
 *
 *  Created on: 15.01.2016
 *      Author: mika
 */

#ifndef EXPERIMENTS_EXP_WORKFLOWS_H_
#define EXPERIMENTS_EXP_WORKFLOWS_H_

#include "cache/experiments/exp_util.h"

namespace cache_exp {
	const time_t timestamp = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	const TemporalReference tref(TIMETYPE_UNIX,timestamp);

	const std::string srtm_wf = R"WF_ESCAPE(
{
  "type": "rasterdb_source",
  "params": {
    "sourcename": "srtm",
    "channel": 0
  }
}
)WF_ESCAPE";


const std::string srtm_ts_wf = R"WF_ESCAPE(
{ "type": "timeshift",
  "params": { "shift": { "from": { "unit": "absolute", "value": "1995-06-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-06-15 12:00:01" } }  },
  "sources": { "raster": [
{
  "type": "rasterdb_source",
  "params": {
    "sourcename": "srtm",
    "channel": 0
  }
}
  ] } 
}
)WF_ESCAPE";

const std::string srtm_ex_wf = R"WF_ESCAPE(
{
  "type": "expression",
  "params":{
		"expression":"(A > 5000) ? 1 : (A>1000) ? 2 : (A > 200) ? 3 : 4",
		"datatype":"Int16",
		"unit": { "measurement": "temperature", "unit": "c", "interpolation":"unknown", "min":1, "max": 4 }
  },
  "sources": { "raster": [
{
  "type": "rasterdb_source",
  "params": {
    "sourcename": "srtm",
    "channel": 0
  }
}
  ] } 
}
)WF_ESCAPE";

	const std::string srtm_proj_wf = R"WF_ESCAPE(
{
  "type": "projection",
  "params": {
    "src_projection": "EPSG:4326",
    "dest_projection": "EPSG:3857"
  },
  "sources": {
    "raster": [
      {
        "type": "rasterdb_source",
        "params": {
          "sourcename": "srtm",
          "channel": 0
        }
      }
    ]
  }
}
)WF_ESCAPE";


	const std::string avg_temp_wf = R"WF_ESCAPE(
{
	"type": "expression",
	"params":{
		"expression":"(A+B+C+D+E+F+G+H+I+J+K+L)/12",
		"datatype":"Float32",
		"unit": { "measurement": "temperature", "unit": "c", "interpolation":"unknown", "min":-100.0, "max": 100.0 }
	},
	"sources" : {
		"raster": [
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-01-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-01-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-02-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-02-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-03-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-03-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-04-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-04-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-05-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-05-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-06-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-06-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-07-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-07-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-08-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-08-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-09-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-09-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-10-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-10-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-11-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-11-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } },
		  { "type": "timeshift",
			"params": { "shift": { "from": { "unit": "absolute", "value": "1995-12-15 12:00:00" }, "to": { "unit": "absolute", "value": "1995-12-15 12:00:01" } }  },
			"sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] } }
	   ]
	}
}
)WF_ESCAPE";


	const std::string cloud_detection_wf = R"WF_ESCAPE( 
{
  "type": "expression",
  "params": {
	"expression": "((K-F<=J)|((A==2)&(((B==2)&(K-M<=15))|((B==1)&(K-M<=18))|(K-I>=2)))|((A==3)&(F-L>1))|((A==3)&(((B==1)&(I-F>7))))|((K<253))|((G<220)|(H<240)|((H-G)<=13))|(((A==1)&(K<261))|(I-K>0)))&(!((A==1)&(E\/C>1.5)))&(!((A==1)&(B==2)&((C-E)\/(C+E)>=0.4)&(K>=265)))",
	"datatype": "Byte",
	"unit": {
	  "measurement": "unknown",
	  "unit": "unknown",
	  "min": 0,
	  "max": 1
	}
  },
  "sources": {
	"raster": [
	  {
		"type": "expression",
		"params": {
		  "expression": "(A<93)?1:((A<100)?2:3)",
		  "datatype": "Byte",
		  "unit": {
			"measurement": "unknown",
			"unit": "unknown",
			"min": 1,
			"max": 3
		  }
		},
		"sources": {
		  "raster": [
			{
			  "type": "msatsolarangle",
			  "params": {
				"solarangle": "zenith"
			  },
			  "sources": {
				"raster": [
				  {
					"type": "rasterdb_source",
					"params": {
					  "sourcename": "msg9_geos",
					  "channel": 8,
					  "transform": false
					}
				  }
				]
			  }
			}
		  ]
		}
	  },
	  {
		"type": "projection",
		"params": {
		  "src_projection": "EPSG:4326",
		  "dest_projection": "EPSG:40453"
		},
		"sources": {
		  "raster": [
			{
			  "type": "reclass",
			  "params": {
				"reclassNoData": true,
				"noDataClass": 1,
				"RemapRange": [
				  [
					-1000,
					0,
					1
				  ],
				  [
					0,
					10000,
					2
				  ]
				]
			  },
			  "sources": {
				"raster": [
				  {
					"type": "rasterdb_source",
					"params": {
					  "sourcename": "srtm",
					  "channel": 0
					}
				  }
				]
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msatreflectance",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 0,
				"transform": true
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msatreflectance",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 1,
				"transform": true
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msatreflectance",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 2,
				"transform": true
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 3,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 4,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 5,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 6,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msatgccthermthresholddetection",
		"sources": {
		  "raster": [
			{
			  "type": "msatsolarangle",
			  "params": {
				"solarangle": "zenith"
			  },
			  "sources": {
				"raster": [
				  {
					"type": "rasterdb_source",
					"params": {
					  "sourcename": "msg9_geos",
					  "channel": 8,
					  "transform": false
					}
				  }
				]
			  }
			},
			{
			  "type": "expression",
			  "params": {
				"expression": "A-B",
				"datatype": "input",
				"unit": {
				  "measurement": "unknown",
				  "unit": "unknown",
				  "min": -50,
				  "max": 50
				}
			  },
			  "sources": {
				"raster": [
				  {
					"type": "msattemperature",
					"sources": {
					  "raster": [
						{
						  "type": "rasterdb_source",
						  "params": {
							"sourcename": "msg9_geos",
							"channel": 8,
							"transform": false
						  }
						}
					  ]
					}
				  },
				  {
					"type": "msattemperature",
					"sources": {
					  "raster": [
						{
						  "type": "rasterdb_source",
						  "params": {
							"sourcename": "msg9_geos",
							"channel": 3,
							"transform": false
						  }
						}
					  ]
					}
				  }
				]
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 8,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 9,
				"transform": false
			  }
			}
		  ]
		}
	  },
	  {
		"type": "msattemperature",
		"sources": {
		  "raster": [
			{
			  "type": "rasterdb_source",
			  "params": {
				"sourcename": "msg9_geos",
				"channel": 10,
				"transform": false
			  }
			}
		  ]
		}
	  }
	]
  }
}
)WF_ESCAPE";


	const QuerySpec cloud_detection(cloud_detection_wf,EPSG_GEOSMSG,CacheType::RASTER,tref, "CloudDetection");
	const QuerySpec avg_temp(avg_temp_wf,EPSG_LATLON,CacheType::RASTER,tref, "Average Temperature");
	const QuerySpec srtm_proj(srtm_proj_wf,EPSG_WEBMERCATOR,CacheType::RASTER,tref, "SRTM Projected");
	const QuerySpec srtm(srtm_wf,EPSG_LATLON,CacheType::RASTER,tref, "SRTM");
	const QuerySpec srtm_ts(srtm_ts_wf,EPSG_LATLON,CacheType::RASTER,tref, "SRTM Timeshifted");
	const QuerySpec srtm_ex(srtm_ex_wf,EPSG_LATLON,CacheType::RASTER,tref, "SRTM Expression");

	static QuerySpec projected_shifted_temp( const std::string &timestamp, const std::string &time_to ) {
		std::string p1 = R"WF_ESCAPE(
		{
		  "type": "projection",
		  "params": {
		    "src_projection": "EPSG:4326",
		    "dest_projection": "EPSG:3857"
		  },
		  "sources": {
		    "raster": [{
		      "type": "timeshift",
			  "params": { "shift": { "from": { "unit": "absolute", "value": ")WF_ESCAPE";


		std::string p2 = R"WF_ESCAPE(" }, "to": { "unit": "absolute", "value": ")WF_ESCAPE";


		std::string p3 = R"WF_ESCAPE(" } }  },
			  "sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] }
		    }]
		  }
		}
		)WF_ESCAPE";

		std::string wf = p1 + timestamp + p2 + time_to + p3;

		return QuerySpec( wf, EPSG_WEBMERCATOR, CacheType::RASTER,tref,"Monthly Temperature (Projected, Shifted to " + timestamp + ")" );
	}

	static QuerySpec shifted_temp( const std::string &timestamp, const std::string &time_to ) {
		std::string p1 = R"WF_ESCAPE(
		{
			  "type": "timeshift",
			  "params": { "shift": { "from": { "unit": "absolute", "value": ")WF_ESCAPE";


		std::string p2 = R"WF_ESCAPE(" }, "to": { "unit": "absolute", "value": ")WF_ESCAPE";


		std::string p3 = R"WF_ESCAPE(" } }  },
			  "sources": { "raster": [{ "type": "rasterdb_source", "params": { "sourcename": "worldclim", "channel": 2 } } ] }
		}
		)WF_ESCAPE";

		std::string wf = p1 + timestamp + p2 + time_to + p3;

		return QuerySpec( wf, EPSG_LATLON, CacheType::RASTER,tref,"Monthly Temperature (Shifted to " + timestamp + ")" );
	}
}

#endif /* EXPERIMENTS_EXP_WORKFLOWS_H_ */
