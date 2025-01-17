/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <memory>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <vector>

#include <grpcpp/impl/channel_argument_option.h>
#include <grpcpp/impl/server_builder_option.h>
#include <grpcpp/impl/server_builder_plugin.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/config.h>

namespace grpc {

std::unique_ptr<ServerBuilderOption> MakeChannelArgumentOption(
    const TString& name, const TString& value) {
  class StringOption final : public ServerBuilderOption {
   public:
    StringOption(const TString& name, const TString& value)
        : name_(name), value_(value) {}

    void UpdateArguments(ChannelArguments* args) override {
      args->SetString(name_, value_);
    }
    void UpdatePlugins(
        std::vector<std::unique_ptr<ServerBuilderPlugin>>* /*plugins*/)
        override {}

   private:
    const TString name_;
    const TString value_;
  };
  return std::unique_ptr<ServerBuilderOption>(new StringOption(name, value));
}

std::unique_ptr<ServerBuilderOption> MakeChannelArgumentOption(
    const TString& name, int value) {
  class IntOption final : public ServerBuilderOption {
   public:
    IntOption(const TString& name, int value)
        : name_(name), value_(value) {}

    void UpdateArguments(ChannelArguments* args) override {
      args->SetInt(name_, value_);
    }
    void UpdatePlugins(
        std::vector<std::unique_ptr<ServerBuilderPlugin>>* /*plugins*/)
        override {}

   private:
    const TString name_;
    const int value_;
  };
  return std::unique_ptr<ServerBuilderOption>(new IntOption(name, value));
}

}  // namespace grpc
