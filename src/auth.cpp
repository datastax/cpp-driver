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
