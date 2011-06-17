/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef MC_KVSTORE_MC_DEBUG_HH
#define MC_KVSTORE_MC_DEBUG_HH

#include <iostream>
#include "memcached/protocol_binary.h"

std::ostream& operator<<(std::ostream& out,
        const protocol_binary_request_header *req);
std::ostream& operator<<(std::ostream& out,
        const protocol_binary_response_header *res);
class Buffer;
std::ostream& operator<<(std::ostream& out, const Buffer *buffer);


#endif
