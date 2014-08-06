#include "QuadTree.h"

using namespace pv;

QuadTree::QuadTree(BoundingBox boundingBox, size_t nodeCapacity) : head(std::make_shared<QuadTreeNode>(boundingBox, nodeCapacity)) {}

void QuadTree::insert(std::shared_ptr<Circle> circle) {
	FindResult probeResult = head->find(*circle);

	if (probeResult.isInsertible()) {
		probeResult.getNode()->insertDirect(circle);
	} else {
		// MERGE
		probeResult.getNode()->removeDirect(probeResult.getCircle());

		Circle mergedCircle = probeResult.getCircle()->merge(*circle);

		insert(std::make_shared<Circle>(mergedCircle));
	}
}

std::vector<std::shared_ptr<Circle>> QuadTree::getCircles() const {
	std::vector<std::shared_ptr<Circle>> circles;
	std::queue<std::shared_ptr<QuadTreeNode>> queue;
	queue.push(head);

	while (!queue.empty()) {
		auto quadTreeNode = queue.front();
		queue.pop();
		auto moreCircles = quadTreeNode->getCircles();
		circles.insert(circles.end(), moreCircles.begin(), moreCircles.end());

		if (quadTreeNode->hasChildren()) {
			queue.push(quadTreeNode->getTopLeft());
			queue.push(quadTreeNode->getTopRight());
			queue.push(quadTreeNode->getBottomLeft());
			queue.push(quadTreeNode->getBottomRight());
		}
	}

	return circles;
}

std::vector<BoundingBox> QuadTree::getBoundingBoxes() const {
	std::vector<BoundingBox> boxes;
	std::queue<std::shared_ptr<QuadTreeNode>> queue;
	queue.push(head);

	while (!queue.empty()) {
		auto quadTreeNode = queue.front();
		queue.pop();

		if (quadTreeNode->hasChildren()) {
			boxes.push_back(quadTreeNode->getBoundingBox());

			queue.push(quadTreeNode->getTopLeft());
			queue.push(quadTreeNode->getTopRight());
			queue.push(quadTreeNode->getBottomLeft());
			queue.push(quadTreeNode->getBottomRight());
		}
	}

	return boxes;
}
