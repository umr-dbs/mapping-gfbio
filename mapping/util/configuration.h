
#include <string>

class Configuration {
	public:
		static void loadFromDefaultPaths();
		static void load(const std::string &filename);
		static const std::string &get(const std::string &name);
		static const std::string &get(const std::string &name, const std::string &defaultValue);
};
