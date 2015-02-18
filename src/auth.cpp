/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "auth.hpp"

namespace cass {

void PlainTextAuthenticator::get_credentials(V1Authenticator::Credentials* credentials) {
  credentials->insert(std::pair<std::string, std::string>("username", username_));
  credentials->insert(std::pair<std::string, std::string>("password", password_));

}

std::string PlainTextAuthenticator::initial_response() {
  std::string token;
  token.reserve(username_.size() + password_.size() + 2);
  token.push_back(0);
  token.append(username_);
  token.push_back(0);
  token.append(password_);
  return token;
}

std::string PlainTextAuthenticator::evaluate_challenge(const std::string& challenge) {
  return std::string();
}

void PlainTextAuthenticator::on_authenticate_success(const std::string& token) {
  // no-op
}

} // namespace cass
