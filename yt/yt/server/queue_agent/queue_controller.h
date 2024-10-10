#pragma once

#include "object.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateQueueController(
    IObjectControllerPtr& controller,
    bool leading,
    const NQueueClient::TQueueTableRow& row,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    const TQueueControllerDynamicConfigPtr& dynamicConfig,
    TQueueAgentClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
