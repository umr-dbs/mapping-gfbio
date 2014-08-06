#ifndef POINTVISUALIZATION_FINDRESULT_H_
#define POINTVISUALIZATION_FINDRESULT_H_

#include <memory>

#include "QuadTreeNode.h"
#include "Circle.h"

namespace pv {

	class QuadTreeNode;

	/**
	 * A result of a find query.
	 * Has either a free spot to directly insert the circle (isInsertible())
	 * or an overlapping circle with corresponding node (hasIntersectingCircle()).
	 */
	class FindResult {
		public:
			/**
			 * Create a FindResult that contains a free spot.
			 * @param node
			 */
			FindResult(std::shared_ptr<QuadTreeNode> node);
			/**
			 * Create a FindResult that contains an overlapping circle and corresponding node.
			 * @param node
			 * @param circle
			 */
			FindResult(std::shared_ptr<QuadTreeNode> node, std::shared_ptr<Circle> circle);

			/**
			 * @return true if the result contains a free spot (node).
			 */
			bool isInsertible() const;
			/**
			 * @return true if the result contains an overlapping circle and corresponding node.
			 */
			bool hasIntersectingCircle() const;

			/**
			 * @return node.
			 */
			std::shared_ptr<QuadTreeNode> getNode() const;
			/**
			 * @return circle (may be std::nullptr)
			 */
			std::shared_ptr<Circle> getCircle() const;

			/**
			 * String representation of the FindResult
			 */
			std::string to_string() const;

		private:
			std::shared_ptr<QuadTreeNode> node;
			std::shared_ptr<Circle> circle;
	};

}

#endif /* POINTVISUALIZATION_FINDRESULT_H_ */
