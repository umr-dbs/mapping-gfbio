#ifndef POINTVISUALIZATION_QUADTREE_H_
#define POINTVISUALIZATION_QUADTREE_H_

#include<vector>
#include<memory>
#include<algorithm>
#include <queue>

#include "BoundingBox.h"
#include "Circle.h"
#include "FindResult.h"
#include "QuadTreeNode.h"

namespace pv {

/**
 * QuadTree that contains non-overlapping cirles.
 * It merges circles automatically upon insert if there are overlapping ones contained in the tree.
 */
class QuadTree {
	public:
		/**
		 * Construct a QuadTree by specifying a bounding box and a node capacity.
		 * @param boundingBox
		 * @param nodeCapacity Specifies the node capacity and split point.
		 */
		QuadTree(BoundingBox boundingBox, size_t nodeCapacity);

		/**
		 * Inserts a circle and merges it if it overlaps with an existing one.
		 * @param circle
		 */
		void insert(std::shared_ptr<Circle> circle);

		/**
		 * Returns all circles from the tree.
		 * @return vector of circles
		 */
		std::vector<std::shared_ptr<Circle>> getCircles() const;

		/**
		 * Get the bounding boxes of all tree nodes.
		 * @return vector of bounding boxes
		 */
		std::vector<BoundingBox> getBoundingBoxes() const;
	private:
		std::shared_ptr<QuadTreeNode> head;
};

}

#endif /* POINTVISUALIZATION_QUADTREE_H_ */
