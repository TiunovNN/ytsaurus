#include "cluster_throttlers_config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TClusterThrottlersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("distributed_throttler", &TThis::DistributedThrottler)
        .DefaultNew();
    registrar.Parameter("cluster_limits", &TThis::ClusterLimits)
        .Default();
    registrar.Parameter("group_id", &TThis::GroupId)
        .Default("/group");
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Postprocessor([] (TClusterThrottlersConfig* config) {
        if (std::ssize(config->GroupId) < 2 || config->GroupId[0] != '/') {
            THROW_ERROR_EXCEPTION("Invalid \"group_id\"")
                << TErrorAttribute("group_id", config->GroupId);
        }

        for (const auto& [clusterName, _] : config->ClusterLimits) {
            if (clusterName.empty()) {
                THROW_ERROR_EXCEPTION("Invalid cluster name %Qv in \"cluster_limist\"",
                    clusterName);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClusterLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bandwidth", &TThis::Bandwidth)
        .DefaultNew();
    registrar.Parameter("rps", &TThis::Rps)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TLimitConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default()
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<NYT::NYson::TYsonString> GetClusterThrottlersYson(NApi::NNative::IClientPtr client)
{
    static constexpr auto ClusterThrottlersConfigPath = "//sys/cluster_throttlers";

    auto errorOrYson = NConcurrency::WaitFor(client->GetNode(ClusterThrottlersConfigPath));
    if (!errorOrYson.IsOK()) {
        return std::nullopt;
    }

    return errorOrYson.Value();
}

TClusterThrottlersConfigPtr MakeClusterThrottlersConfig(const NYT::NYson::TYsonString& yson)
{
    auto config = New<TClusterThrottlersConfig>();
    config->Load(NYTree::ConvertTo<NYTree::INodePtr>(yson));
    return config;
}

TClusterThrottlersConfigPtr GetClusterThrottlersConfig(NApi::NNative::IClientPtr client)
{
    auto yson = GetClusterThrottlersYson(client);
    if (!yson) {
        return nullptr;
    }

    return MakeClusterThrottlersConfig(*yson);
}

bool AreClusterThrottlersConfigsEqual(TClusterThrottlersConfigPtr lhs, TClusterThrottlersConfigPtr rhs)
{
    if (!lhs && !rhs) {
        return true;
    }

    if (!lhs != !rhs) {
        return false;
    }

    return *lhs == *rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
