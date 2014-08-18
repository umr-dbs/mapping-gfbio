#ifndef POINTVISUALIZATION_QUADTREENODE_H_
#define POINTVISUALIZATION_QUADTREENODE_H_

#include <vector>
#include <memory>
#include <algorithm>
#include <array>
#include <string>
#include <sstream>

#include "BoundingBox.h"
#include "Circle.h"
#include "FindResult.h"

namespace pv {

	class FindResult;

	/**
	 * QuadTree node, may contain again 4 children.
	 */
	class QuadTreeNode : public std::enable_shared_from_this<QuadTreeNode> {
		public:
			/**
			 * Construct a QuadTreeNode by specifying a bounding box and a node capacity.
			 * @param boundingBox
			 * @param nodeCapacity Specifies the node capacity and split point.
			 */
			QuadTreeNode(BoundingBox boundingBox, size_t nodeCapacity);

			/**
			 * Finds a free spot in the tree or an overlapping circle.
			 * @param circle
			 * @return FindResult
			 */
			FindResult find(Circle& circle);

			/**
			 * Inserts the circle directly into this node (does not look into children).
			 * Splits the node if necessary.
			 * @param circle
			 */
			void insertDirect(std::shared_ptr<Circle> circle);

			/**
			 * Removes the circle directly from this node.
			 * Does not look into children nodes.
			 * @param circle
			 */
			void removeDirect(std::shared_ptr<Circle> circle);

			/**
			 * Inserts the circle if it is completely contained within the node's bounds.
			 * @param circle
			 * @return true if the circle was inserted, false else
			 */
			bool insertIfInBounds(std::shared_ptr<Circle> circle);

			/**
			 * Get all circles from this node.
			 * @param circle
			 * @return vector of circles
			 */
			std::vector<std::shared_ptr<Circle>> getCircles() const;

			/**
			 * Get the bounding box of this node.
			 * @return BoundingBox
			 */
			BoundingBox getBoundingBox() const;

			/**
			 * @return top left child node (may be std::nullptr)
			 */
			std::shared_ptr<QuadTreeNode> getTopLeft() const;

			/**
			 * @return top right child node (may be std::nullptr)
			 */
			std::shared_ptr<QuadTreeNode> getTopRight() const;

			/**
			 * @return bottom left child node (may be std::nullptr)
			 */
			std::shared_ptr<QuadTreeNode> getBottomLeft() const;

			/**
			 * @return bottom right child node (may be std::nullptr)
			 */
			std::shared_ptr<QuadTreeNode> getBottomRight() const;

			/**
			 * Checks if the node has children.
			 * @return true if the node was splitted (has children nodes), false else
			 */
			bool hasChildren() const;

			/**
			 * String representation of the QuadTreeNode
			 */
			std::string to_string() const;

		private:
			const BoundingBox boundingBox;
			const size_t nodeCapacity;

			std::array<std::shared_ptr<QuadTreeNode>, 4> children;

			std::vector<std::shared_ptr<Circle>> circles;

			void split();
	};

}

#endif /* POINTVISUALIZATION_QUADTREENODE_H_ */
