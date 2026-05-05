#pragma once

#include <string>
#include "common/message.h"

namespace kafka {

/// Phân tích payload (body) thành đối tượng Request tương ứng dựa vào api_key
bool parse_request_body(int16_t api_key, const std::string& body, ReqType& req_type, Request& req);

/// Xử lý một request không hỗ trợ và trả về byte array (chuỗi bytes) để gửi báo lỗi
std::string serialize_unsupported_error(const std::string& body);

} // namespace kafka
