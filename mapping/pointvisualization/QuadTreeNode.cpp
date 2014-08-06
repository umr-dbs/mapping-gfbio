#include "QuadTreeNode.h"

using namespace pv;

QuadTreeNode::QuadTreeNode(BoundingBox boundingBox, size_t nodeCapacity) : boundingBox(boundingBox), nodeCapacity(nodeCapacity) {
	circles.reserve(nodeCapacity);
}

FindResult QuadTreeNode::find(Circle& circle) {
	// check circles
	for (auto& nodeCircle : circles) {
		if (circle.intersects(*nodeCircle)) {
			return FindResult(shared_from_this(), nodeCircle); // intersecting circle
		}
	}

	// check children responsibilities
	if (hasChildren()) {
		std::shared_ptr<Circle> intersectingCircle;
		for (auto& child : children) {
			if (child->getBoundingBox().intersects(circle)) {
				FindResult result = child->find(circle);
				if (result.hasIntersectingCircle()) {
					return result;
				} else if (child->getBoundingBox().contains(circle)) {
					return result; // node is responsible for circle and may or may not have an intersecting point
				} else if(result.hasIntersectingCircle()) {
					intersectingCircle = result.getCircle();
				}
			}
		}

		if (intersectingCircle != nullptr) {
			return FindResult(shared_from_this(), intersectingCircle);
		} else {
			return FindResult(shared_from_this());
		}
	}

	return FindResult(shared_from_this()); // node is free
}

void QuadTreeNode::insertDirect(std::shared_ptr<Circle> circle) {
	if (hasChildren()) {
		circles.push_back(circle);
	} else {
		if (circles.size() < nodeCapacity) {
			circles.push_back(circle);
		} else {
			split();
			
			bool insertSuccessful = false;
			for (auto& child : children) {
				if (child->insertIfInBounds(circle)) {
					insertSuccessful = true;
					break;
				}
			}

			if (!insertSuccessful) {
				circles.push_back(circle);
			}
		}
	}
}

void QuadTreeNode::removeDirect(std::shared_ptr<Circle> circle) {
	circles.erase(std::remove(circles.begin(), circles.end(), circle), circles.end());
}

bool QuadTreeNode::insertIfInBounds(std::shared_ptr<Circle> circle) {
	if (boundingBox.contains(*circle)) {
		insertDirect(circle);
		return true;
	} else {
		return false;
	}
}

void QuadTreeNode::split() {
	Dimension newDimension = boundingBox.getHalfDimension().halve();

	children[0] = std::make_shared<QuadTreeNode>(
		BoundingBox(Coordinate(boundingBox.getCenter().getX() - newDimension.getWidth(), boundingBox.getCenter().getY() - newDimension.getHeight()), newDimension, boundingBox.getEpsilonDistance()),
		nodeCapacity
		);

	children[1] = std::make_shared<QuadTreeNode>(
		BoundingBox(Coordinate(boundingBox.getCenter().getX() + newDimension.getWidth(), boundingBox.getCenter().getY() - newDimension.getHeight()), newDimension, boundingBox.getEpsilonDistance()),
		nodeCapacity
		);

	children[2] = std::make_shared<QuadTreeNode>(
		BoundingBox(Coordinate(boundingBox.getCenter().getX() - newDimension.getWidth(), boundingBox.getCenter().getY() + newDimension.getHeight()), newDimension, boundingBox.getEpsilonDistance()),
		nodeCapacity
		);

	children[3] = std::make_shared<QuadTreeNode>(
		BoundingBox(Coordinate(boundingBox.getCenter().getX() + newDimension.getWidth(), boundingBox.getCenter().getY() + newDimension.getHeight()), newDimension, boundingBox.getEpsilonDistance()),
		nodeCapacity
		);

	circles.erase(std::remove_if(circles.begin(), circles.end(), [this](std::shared_ptr<Circle> circle) {
		return getTopLeft()->insertIfInBounds(circle) || getTopRight()->insertIfInBounds(circle) || getBottomLeft()->insertIfInBounds(circle) || getBottomRight()->insertIfInBounds(circle);
	}), circles.end());
}

std::vector<std::shared_ptr<Circle>> QuadTreeNode::getCircles() const {
	return circles;
}

BoundingBox QuadTreeNode::getBoundingBox() const {
	return boundingBox;
}

bool QuadTreeNode::hasChildren() const {
	return children[0] != nullptr;
}

std::shared_ptr<QuadTreeNode> QuadTreeNode::getTopLeft() const {
	return children[0];//topLeft;
}

std::shared_ptr<QuadTreeNode> QuadTreeNode::getTopRight() const {
	return children[1];//topRight;
}

std::shared_ptr<QuadTreeNode> QuadTreeNode::getBottomLeft() const {
	return children[2];//bottomLeft;
}

std::shared_ptr<QuadTreeNode> QuadTreeNode::getBottomRight() const {
	return children[3];//bottomRight;
}

std::string QuadTreeNode::to_string() const {
	std::stringstream toString;
	toString << boundingBox.to_string();
	return toString.str();
}
