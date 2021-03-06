licenses(["notice"])  # Apache 2

load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

envoy_package()

envoy_cc_library(
    name = "address_json_lib",
    srcs = ["address_json.cc"],
    hdrs = ["address_json.h"],
    external_deps = ["envoy_address"],
    deps = [
        "//include/envoy/json:json_object_interface",
        "//source/common/common:assert_lib",
        "//source/common/network:utility_lib",
    ],
)

envoy_cc_library(
    name = "ads_api_lib",
    hdrs = ["ads_api_impl.h"],
    external_deps = ["envoy_discovery"],
    deps = [
        "//include/envoy/config:ads_interface",
        "//include/envoy/config:subscription_interface",
        "//include/envoy/upstream:cluster_manager_interface",
    ],
)

envoy_cc_library(
    name = "ads_subscription_lib",
    hdrs = ["ads_subscription_impl.h"],
    external_deps = ["envoy_discovery"],
    deps = [
        "//include/envoy/config:ads_interface",
        "//include/envoy/config:subscription_interface",
        "//source/common/common:assert_lib",
        "//source/common/common:logger_lib",
        "//source/common/grpc:common_lib",
        "//source/common/protobuf",
    ],
)

envoy_cc_library(
    name = "bootstrap_json_lib",
    srcs = ["bootstrap_json.cc"],
    hdrs = ["bootstrap_json.h"],
    external_deps = ["envoy_bootstrap"],
    deps = [
        ":address_json_lib",
        ":cds_json_lib",
        ":json_utility_lib",
        ":lds_json_lib",
        ":utility_lib",
        "//include/envoy/json:json_object_interface",
        "//source/common/common:assert_lib",
        "//source/common/config:well_known_names",
        "//source/common/json:config_schemas_lib",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "base_json_lib",
    srcs = ["base_json.cc"],
    hdrs = ["base_json.h"],
    external_deps = ["envoy_base"],
    deps = [
        "//include/envoy/json:json_object_interface",
    ],
)

envoy_cc_library(
    name = "cds_json_lib",
    srcs = ["cds_json.cc"],
    hdrs = ["cds_json.h"],
    external_deps = ["envoy_cds"],
    deps = [
        ":address_json_lib",
        ":json_utility_lib",
        ":protocol_json_lib",
        ":tls_context_json_lib",
        ":utility_lib",
        "//include/envoy/common:optional",
        "//include/envoy/json:json_object_interface",
        "//include/envoy/upstream:cluster_manager_interface",
        "//source/common/common:assert_lib",
        "//source/common/json:config_schemas_lib",
        "//source/common/network:utility_lib",
    ],
)

envoy_cc_library(
    name = "filesystem_subscription_lib",
    hdrs = ["filesystem_subscription_impl.h"],
    deps = [
        "//include/envoy/config:subscription_interface",
        "//include/envoy/event:dispatcher_interface",
        "//include/envoy/filesystem:filesystem_interface",
        "//source/common/common:logger_lib",
        "//source/common/config:utility_lib",
        "//source/common/protobuf",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "filter_json_lib",
    srcs = ["filter_json.cc"],
    hdrs = ["filter_json.h"],
    external_deps = ["envoy_filter_http_connection_manager"],
    deps = [
        ":json_utility_lib",
        ":protocol_json_lib",
        ":rds_json_lib",
        ":utility_lib",
        "//include/envoy/json:json_object_interface",
        "//source/common/common:assert_lib",
        "//source/common/common:utility_lib",
        "//source/common/config:well_known_names",
        "//source/common/json:config_schemas_lib",
        "//source/common/protobuf",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "grpc_subscription_lib",
    hdrs = ["grpc_subscription_impl.h"],
    external_deps = ["envoy_base"],
    deps = [
        "//include/envoy/config:subscription_interface",
        "//source/common/common:logger_lib",
        "//source/common/config:utility_lib",
        "//source/common/grpc:async_client_lib",
        "//source/common/protobuf",
    ],
)

envoy_cc_library(
    name = "http_subscription_lib",
    hdrs = ["http_subscription_impl.h"],
    external_deps = [
        "envoy_base",
        "http_api_protos",
    ],
    deps = [
        "//include/envoy/config:subscription_interface",
        "//source/common/buffer:buffer_lib",
        "//source/common/common:logger_lib",
        "//source/common/config:utility_lib",
        "//source/common/http:headers_lib",
        "//source/common/http:rest_api_fetcher_lib",
        "//source/common/protobuf",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "json_utility_lib",
    hdrs = ["json_utility.h"],
    deps = [
        "//include/envoy/json:json_object_interface",
        "//source/common/protobuf",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "lds_json_lib",
    srcs = ["lds_json.cc"],
    hdrs = ["lds_json.h"],
    external_deps = ["envoy_lds"],
    deps = [
        ":address_json_lib",
        ":json_utility_lib",
        ":tls_context_json_lib",
        ":well_known_names",
        "//include/envoy/json:json_object_interface",
        "//source/common/common:assert_lib",
        "//source/common/json:config_schemas_lib",
        "//source/common/network:utility_lib",
    ],
)

envoy_cc_library(
    name = "metadata_lib",
    srcs = ["metadata.cc"],
    hdrs = ["metadata.h"],
    external_deps = ["envoy_base"],
    deps = [
        "//source/common/common:singleton",
        "//source/common/protobuf",
    ],
)

envoy_cc_library(
    name = "protocol_json_lib",
    srcs = ["protocol_json.cc"],
    hdrs = ["protocol_json.h"],
    external_deps = ["envoy_protocol"],
    deps = [
        ":json_utility_lib",
        "//include/envoy/json:json_object_interface",
    ],
)

envoy_cc_library(
    name = "rds_json_lib",
    srcs = ["rds_json.cc"],
    hdrs = ["rds_json.h"],
    external_deps = ["envoy_rds"],
    deps = [
        ":base_json_lib",
        ":json_utility_lib",
        ":metadata_lib",
        ":well_known_names",
        "//include/envoy/json:json_object_interface",
        "//source/common/common:assert_lib",
        "//source/common/json:config_schemas_lib",
    ],
)

envoy_cc_library(
    name = "subscription_factory_lib",
    hdrs = ["subscription_factory.h"],
    external_deps = ["envoy_base"],
    deps = [
        ":ads_subscription_lib",
        ":filesystem_subscription_lib",
        ":grpc_subscription_lib",
        ":http_subscription_lib",
        ":utility_lib",
        "//include/envoy/config:subscription_interface",
        "//source/common/protobuf",
    ],
)

envoy_cc_library(
    name = "tls_context_json_lib",
    srcs = ["tls_context_json.cc"],
    hdrs = ["tls_context_json.h"],
    external_deps = ["envoy_tls_context"],
    deps = [
        ":json_utility_lib",
        "//include/envoy/json:json_object_interface",
        "//source/common/common:utility_lib",
    ],
)

envoy_cc_library(
    name = "utility_lib",
    srcs = ["utility.cc"],
    hdrs = ["utility.h"],
    external_deps = [
        "envoy_base",
        "envoy_filter_http_connection_manager",
    ],
    deps = [
        ":json_utility_lib",
        "//include/envoy/config:subscription_interface",
        "//include/envoy/local_info:local_info_interface",
        "//include/envoy/registry",
        "//include/envoy/upstream:cluster_manager_interface",
        "//source/common/common:assert_lib",
        "//source/common/common:hash_lib",
        "//source/common/common:hex_lib",
        "//source/common/common:singleton",
        "//source/common/json:config_schemas_lib",
        "//source/common/protobuf",
        "//source/common/protobuf:utility_lib",
    ],
)

envoy_cc_library(
    name = "well_known_names",
    hdrs = ["well_known_names.h"],
    deps = ["//source/common/common:singleton"],
)
