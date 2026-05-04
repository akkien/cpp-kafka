#include "common/message.h"

struct RequestItem {
    int32_t client_fd;
    ReqType req_type;
    Request request;
};