#include "FindResult.h"

using namespace pv;

FindResult::FindResult(std::shared_ptr<QuadTreeNode> node) : node(node) {}
FindResult::FindResult(std::shared_ptr<QuadTreeNode> node, std::shared_ptr<Circle> circle) : node(node), circle(circle) {}

bool FindResult::isInsertible() const {
	return (node != nullptr) && (circle == nullptr);
}

bool FindResult::hasIntersectingCircle() const {
	return circle != nullptr;
}

std::shared_ptr<QuadTreeNode> FindResult::getNode() const {
	return node;
}

std::shared_ptr<Circle> FindResult::getCircle() const {
	return circle;
}

std::string FindResult::to_string() const {
	std::stringstream toString;
	toString << "Node: " << node->to_string();
	if (hasIntersectingCircle()) {
		toString << ", Circle: " << circle->to_string();
	}
	return toString.str();
}