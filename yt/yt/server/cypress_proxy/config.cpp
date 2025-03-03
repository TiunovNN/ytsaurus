#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("root_path", &TThis::RootPath)
        .Default("//sys/cypress_proxies");

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Parameter("user_directory_synchronizer", &TThis::UserDirectorySynchronizer)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (!config->DynamicConfigPath) {
            config->DynamicConfigPath = config->RootPath + "/@config";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(1);
    registrar.Parameter("allow_bypass_master_resolve", &TThis::AllowBypassMasterResolve)
        .Default(false);
    registrar.Parameter("alert_on_mixed_read_write_batch", &TThis::AlertOnMixedReadWriteBatch)
        .Default(false);

    registrar.Parameter("distributed_throttler", &TThis::DistributedThrottler)
        .DefaultNew();

    registrar.Parameter("enable_per_user_request_weight_throttling", &TThis::EnablePerUserRequestWeightThrottling)
        .Default(true);
    registrar.Parameter("default_per_user_read_request_weight_throttler_config", &TThis::DefaultPerUserReadRequestWeightThrottlerConfig)
        .DefaultNew();
    registrar.Parameter("default_per_user_write_request_weight_throttler_config", &TThis::DefaultPerUserWriteRequestWeightThrottlerConfig)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(
            config->DistributedThrottler->Mode == NDistributedThrottler::EDistributedThrottlerMode::Precise,
            "Cypress proxies distributed throttler's mode cannot be set to %Qv",
            NDistributedThrottler::EDistributedThrottlerMode::Precise);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaResponseKeeperDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TUserDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("sync_splay", &TThis::SyncSplay)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("object_service", &TThis::ObjectService)
        .DefaultNew();
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
