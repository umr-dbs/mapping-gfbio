#ifndef RSERVER_RSERVER_H
#define RSERVER_RSERVER_H

const int RSERVER_MAGIC_NUMBER = 0x12345678;
const char RSERVER_TYPE_RASTER = 1;
const char RSERVER_TYPE_POINTS = 2;
const char RSERVER_TYPE_STRING = 3;
const char RSERVER_TYPE_PLOT = 4;
const char RSERVER_TYPE_ERROR = 99;


/*
 * The Socket protocol is as follows:
 *
 * The Client sends a request:
 * 1) (string) desired result type (raster, points, text, plot)
 * 2) (string) source code
 * 3) (int) numbers of rastersources and pointsources
 * 4) (QueryRectangle) the queryrectangle to be used
 *
 * The Server then starts running the R script and will reply with multiple packets, each starting with
 *
 * 1) (char): type of packet
 * -> if positive and equal to one of the constants above (RSERVER_TYPE_RASTER, ...), then it's a
 *    request to load an object from a source
 *    2) (int) source-index
 *    3) (QueryRectangle)
 *    Client responds with a single object of the requested type
 * -> if negative (-RSERVER_TYPE_RASTER, ..), then it's the final output, followed by a single object
 *    of the requested type, followed by the server closing the connection
 */

#endif
