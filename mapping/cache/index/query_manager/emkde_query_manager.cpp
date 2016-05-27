/*
 * bema_query_manager.cpp
 *
 *  Created on: 24.05.2016
 *      Author: koerberm
 */

#include "cache/index/query_manager/emkde_query_manager.h"
#include "util/log.h"
#include "util/exceptions.h"

const GDAL::CRSTransformer EMKDEQueryManager::TRANS_GEOSMSG(EPSG_GEOSMSG,EPSG_LATLON);
const GDAL::CRSTransformer EMKDEQueryManager::TRANS_WEBMERCATOR(EPSG_WEBMERCATOR,EPSG_LATLON);
const uint32_t EMKDEQueryManager::MAX_Z = 0xFFFFFFFF;
const uint32_t EMKDEQueryManager::MASKS[] = {0x55555555, 0x33333333, 0x0F0F0F0F, 0x00FF00FF};
const uint32_t EMKDEQueryManager::SHIFTS[] = {1, 2, 4, 8};
const uint16_t EMKDEQueryManager::SCALE_X = 0xFFFF / 360;
const uint16_t EMKDEQueryManager::SCALE_Y = 0xFFFF / 180;

EMKDEQueryManager::EMKDEQueryManager(
		const std::map<uint32_t, std::shared_ptr<Node> >& nodes) : SimpleQueryManager(nodes),
				alpha(0.3), bandwith(6) {
	bins.fill(0);
}

std::unique_ptr<PendingQuery> EMKDEQueryManager::create_job(
		const BaseRequest& req) {

	check_nodes_changed();

	uint32_t node = 0;

	auto hv = get_hilbert_value(req.query);
	for ( auto &n : bounds ) {
		if ( hv <= n.hilbert_bound ) {
			node = n.node_id;
			break;
		}
	}
	if ( node == 0 )
		throw MustNotHappenException("No node found to schedule job on!");

	double fsum = update_bins(hv);
	update_bounds(fsum);
	return make_unique<SimpleJob>(req,node);
}

double EMKDEQueryManager::update_bins(uint32_t hv) {
	uint32_t selected = hv / (MAX_Z / bins.size());
	double fsum = 0;
	for ( uint32_t i = 0; i < bins.size(); i++ ) {
		if ( i >= (selected-bandwith/2) && i <= (selected+bandwith/2) ) {
			bins[i] = bins[i] * (1.0-alpha) + alpha / (bandwith+1);
		}
		else
			bins[i] *= (1.0-alpha);
		fsum += bins[i];
	}
	return fsum;
}

void EMKDEQueryManager::check_nodes_changed() {
	if ( nodes.size() != bounds.size() ) {
		bounds.clear();
		for ( auto &p : nodes ) {
			bounds.push_back( EMNode(p.first) );
		}

		double sum = 0;
		for ( auto &f : bins )
			sum += f;
		update_bounds(sum);
	}
}

void EMKDEQueryManager::update_bounds(double fsum) {


	if ( fsum == 0 ) {
		for ( auto &n : bounds )
			n.hilbert_bound = MAX_Z;
		return;
	}

	double per_node = fsum / bounds.size();
	double bin_width = MAX_Z / bins.size();
	double f = bins[0];

	double sum = 0;
	double bound = 0;
	double interp = 0;


	size_t s = 0;
	size_t i = 0;

	while ( i < bins.size() ) {
		if ( sum + f <= per_node ) {
			sum += f;
			f = bins[++i];
			bin_width = MAX_Z / bins.size();
			bound = bin_width * i;
		}
		else {
			interp = (per_node-sum)/f;
			bound += interp*bin_width;
			bounds[s++].hilbert_bound = bound;
			f = f- (per_node-sum);
			bin_width -= interp*bin_width;
			sum = 0;
		}
	}
	std::ostringstream ss;
	ss << "Bounds: [";
	for ( auto &en : bounds ) {
		ss << en.node_id << ": " << en.hilbert_bound << ", ";
	}
	ss << "]";
	Log::debug("%s", ss.str().c_str());
}

uint32_t EMKDEQueryManager::get_hilbert_value(const QueryRectangle& rect) {
	double ex = rect.x1 + (rect.x2-rect.x1)/2.0;
	double ey = rect.y1 + (rect.y2-rect.y1)/2.0;

	if ( rect.epsg == EPSG_GEOSMSG ) {
		EMKDEQueryManager::TRANS_GEOSMSG.transform(ex,ey);
	}
	else if ( rect.epsg == EPSG_WEBMERCATOR ) {
		EMKDEQueryManager::TRANS_WEBMERCATOR.transform(ex,ey);
	}

	// Translate and scale
	uint32_t x = (ex+180) * SCALE_X;
	uint32_t y = (ey+ 90) * SCALE_Y;

	x = (x | (x << SHIFTS[3])) & MASKS[3];
	x = (x | (x << SHIFTS[2])) & MASKS[2];
	x = (x | (x << SHIFTS[1])) & MASKS[1];
	x = (x | (x << SHIFTS[0])) & MASKS[0];

	y = (y | (y << SHIFTS[3])) & MASKS[3];
	y = (y | (y << SHIFTS[2])) & MASKS[2];
	y = (y | (y << SHIFTS[1])) & MASKS[1];
	y = (y | (y << SHIFTS[0])) & MASKS[0];

	uint32_t result = x | (y << 1);
	return result;
}
