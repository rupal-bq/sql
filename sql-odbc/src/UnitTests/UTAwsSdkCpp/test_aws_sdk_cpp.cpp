/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

// clang-format off
#include "pch.h"
#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif  // __APPLE__
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#ifdef __APPLE__
#pragma clang diagnostic pop
#endif  // __APPLE__
/*
#include "unit_test_helper.h"

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Http;

static const char service_name[] = "es";
static const char allocation_tag[] = "AWS_SIGV4_Test";
static const char host[] = "https://search-bit-quill-cx3hpfoxvasohujxkllmgjwqde.us-west-2.es.amazonaws.com";
static const char region[] = "us-west-2";

TEST(AWS_SIGV4, EnvironmentAWSCredentials) {
	Aws::SDKOptions options;
	EXPECT_NO_THROW(Aws::InitAPI(options));

	auto request = CreateHttpRequest(Aws::String(host), HttpMethod::HTTP_GET, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

	std::shared_ptr<EnvironmentAWSCredentialsProvider> credential_provider =  Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(allocation_tag);

	AWSAuthV4Signer signer(credential_provider, service_name, region);
	ASSERT_TRUE(signer.SignRequest(*request));

	auto http_client = CreateHttpClient(Aws::Client::ClientConfiguration());

	auto response = http_client->MakeRequest(request);
	ASSERT_NE(response, nullptr);
	EXPECT_EQ(Aws::Http::HttpResponseCode::OK, response->GetResponseCode());
	
	EXPECT_NO_THROW(Aws::ShutdownAPI(options));
}

TEST(SettingSDKOptions, TurnLoggingOn) {
    Aws::SDKOptions options;
	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    EXPECT_NO_THROW(Aws::InitAPI(options));
    EXPECT_NO_THROW(Aws::ShutdownAPI(options));
}
*/

TEST(TestSSL, disableVerifySSL) {
	Aws::SDKOptions options;
	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    EXPECT_NO_THROW(Aws::InitAPI(options));
    
    // Initialize logging
    Aws::Utils::Logging::InitializeAWSLogging(
    Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
        "AwsVerifySslTest", Aws::Utils::Logging::LogLevel::Trace, "aws_sdk_"));

    // Set client configuration
	Aws::Client::ClientConfiguration config;
	config.scheme = Aws::Http::Scheme::HTTPS;
	config.verifySSL = false;

	std::shared_ptr< Aws::Http::HttpClient > http_client = Aws::Http::CreateHttpClient(config);

    // Generate http request
	std::shared_ptr< Aws::Http::HttpRequest > request =
        Aws::Http::CreateHttpRequest(
            Aws::String("https://localhost:9200/_cat/plugins?format=json"),
            Aws::Http::HttpMethod::HTTP_GET,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

    // Set Authentication
	std::string userpw_str = "admin:admin";
    Aws::Utils::Array< unsigned char > userpw_arr(
            reinterpret_cast< const unsigned char* >(userpw_str.c_str()),
            userpw_str.length());
    std::string hashed_userpw =
            Aws::Utils::HashingUtils::Base64Encode(userpw_arr);
    request->SetAuthorization("Basic " + hashed_userpw);

    // Issue request 
    std::shared_ptr< Aws::Http::HttpResponse > response = http_client->MakeRequest(request);

    EXPECT_EQ(response->GetResponseCode(), Aws::Http::HttpResponseCode::OK);
    EXPECT_NO_THROW(Aws::Utils::Logging::ShutdownAWSLogging());
    EXPECT_NO_THROW(Aws::ShutdownAPI(options));
}

int main(int argc, char** argv) {
    //testing::internal::CaptureStdout();
    ::testing::InitGoogleTest(&argc, argv);
    int failures = RUN_ALL_TESTS();
    //std::string output = testing::internal::GetCapturedStdout();
    //std::cout << output << std::endl;
    //std::cout << (failures ? "Not all tests passed." : "All tests passed") << std::endl;
    //WriteFileIfSpecified(argv, argv + argc, "-fout", output);
    return failures;
}
