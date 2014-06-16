
#include <curl/curl.h>
#include <string>

class cURL {
	typedef size_t (*write_callback)(void *buffer, size_t size, size_t nmemb, void *userp);
	typedef size_t (*read_callback)(char *bufptr, size_t size, size_t nitems, void *userp);


	public:
		cURL();
		~cURL();
		template<typename T>
			void setOpt(CURLoption option, T value) {
				curl_easy_setopt(handle, option, value);
			};
		void perform();
		std::string escape(const std::string &input);

		static size_t defaultWriteFunction(void *buffer, size_t size, size_t nmemb, void *userp);
	private:
		CURL *handle;
		char errorbuffer[CURL_ERROR_SIZE];
};
