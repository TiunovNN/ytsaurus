#include "chunk_placement.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "config.h"
#include "job.h"
#include "chunk_location.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/host.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/server/master/object_server/object.h>

#include <util/random/random.h>

#include <array>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement::TTargetCollector
{
public:
    TTargetCollector(
        const TChunkPlacement* chunkPlacement,
        const TMedium* medium,
        const TChunk* chunk,
        std::optional<int> replicationFactorOverride,
        bool allowMultipleReplicasPerNode,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica)
        : ChunkPlacement_(chunkPlacement)
        , Medium_(medium)
        , Chunk_(chunk)
        , MaxReplicasPerRack_(ChunkPlacement_->GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride))
        , ReplicationFactorOverride_(replicationFactorOverride)
        , AllowMultipleReplicasPerNode_(allowMultipleReplicasPerNode)
    {
        if (forbiddenNodes) {
            ForbiddenNodes_ = *forbiddenNodes;
        }
        if (allocatedNodes) {
            ForbiddenNodes_.insert(
                ForbiddenNodes_.end(),
                allocatedNodes->begin(),
                allocatedNodes->end());
        }

        SortUnique(ForbiddenNodes_);

        auto processAllocatedNode = [&] (TNode* node) {
            IncreaseRackUsage(node);
            IncreaseDataCenterUsage(node);
        };

        int mediumIndex = medium->GetIndex();
        for (auto replica : chunk->StoredReplicas()) {
            if (replica.GetPtr()->GetEffectiveMediumIndex() == mediumIndex) {
                auto* node = GetChunkLocationNode(replica);
                if (!AllowMultipleReplicasPerNode_) {
                    ForbiddenNodes_.push_back(node);
                }
                // NB: When running replication job for unsafely placed chunk we do not increment
                // counters for unsafely placed replica because it will be removed anyway. Otherwise
                // it is possible that no feasible replica will be found. Consider case with three
                // storage data centers and RS(3, 3) chunk. Data center replica count limit forbids to
                // put more than two replicas in every data center, so it's impossible to allocate extra
                // replica to move unsafely placed replica there.
                if (!node->IsDecommissioned() && replica != unsafelyPlacedReplica) {
                    processAllocatedNode(node);
                }
            }
        }

        if (allocatedNodes) {
            for (auto* node : *allocatedNodes) {
                processAllocatedNode(node);
            }
        }
    }

    bool CheckNode(
        TNode* node,
        bool enableRackAwareness,
        bool enableDataCenterAwareness) const
    {
        if (std::find(ForbiddenNodes_.begin(), ForbiddenNodes_.end(), node) != ForbiddenNodes_.end()) {
            return false;
        }

        if (enableRackAwareness && !CheckRackUsage(node)) {
            return false;
        }

        if (enableDataCenterAwareness && !CheckDataCenterUsage(node)) {
            return false;
        }

        return true;
    }

    void AddNode(TNode* node)
    {
        IncreaseRackUsage(node);
        IncreaseDataCenterUsage(node);
        AddedNodes_.push_back(node);
        if (!AllowMultipleReplicasPerNode_) {
            ForbiddenNodes_.push_back(node);
        }
    }

    const TNodeList& GetAddedNodes() const
    {
        return AddedNodes_;
    }

private:
    const TChunkPlacement* const ChunkPlacement_;

    const TMedium* const Medium_;
    const TChunk* const Chunk_;

    const int MaxReplicasPerRack_;
    const std::optional<int> ReplicationFactorOverride_;
    const bool AllowMultipleReplicasPerNode_;

    std::array<i8, RackIndexBound> PerRackCounters_{};

    // TODO(gritukan): YT-16557
    TCompactFlatMap<const TDataCenter*, i8, 4> PerDataCenterCounters_;

    TNodeList ForbiddenNodes_;
    TNodeList AddedNodes_;

    void IncreaseRackUsage(TNode* node)
    {
        const auto* rack = node->GetRack();
        if (rack) {
            ++PerRackCounters_[rack->GetIndex()];
        }
    }

    bool CheckRackUsage(TNode* node) const
    {
        if (const auto* rack = node->GetRack()) {
            auto usage = PerRackCounters_[rack->GetIndex()];
            return usage < MaxReplicasPerRack_;
        } else {
            return true;
        }
    }

    void IncreaseDataCenterUsage(TNode* node)
    {
        if (const auto* dataCenter = node->GetDataCenter()) {
            auto counterIt = PerDataCenterCounters_.find(dataCenter);
            if (counterIt == PerDataCenterCounters_.end()) {
                PerDataCenterCounters_.emplace(dataCenter, 1);
            } else {
                ++counterIt->second;
            }
        }
    }

    bool CheckDataCenterUsage(TNode* node) const
    {
        auto* dataCenter = node->GetDataCenter();
        YT_ASSERT(dataCenter);
        auto counterIt = PerDataCenterCounters_.find(dataCenter);
        if (counterIt == PerDataCenterCounters_.end()) {
            return true;
        }

        auto counter = counterIt->second;
        auto maxReplicasPerDataCenter = GetMaxReplicasPerDataCenter(dataCenter);
        return counter < maxReplicasPerDataCenter;
    }

    int GetMaxReplicasPerDataCenter(TDataCenter* dataCenter) const
    {
        return ChunkPlacement_->GetMaxReplicasPerDataCenter(
            Medium_,
            Chunk_,
            dataCenter,
            ReplicationFactorOverride_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TBootstrap* bootstrap,
    TConsistentChunkPlacementPtr consistentPlacement)
    : Bootstrap_(bootstrap)
    , Config_(bootstrap->GetConfig()->ChunkManager)
    , ConsistentPlacement_(std::move(consistentPlacement))
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TChunkPlacement::OnDynamicConfigChanged, MakeWeak(this)));
}

void TChunkPlacement::Clear()
{
    MediumToLoadFactorToNode_.clear();
    IsDataCenterAware_ = false;
    StorageDataCenters_.clear();
    BannedStorageDataCenters_.clear();
    AliveStorageDataCenters_.clear();
    DataCenterSetErrors_.clear();
}

void TChunkPlacement::Initialize()
{
    const auto& nodes = Bootstrap_->GetNodeTracker()->Nodes();
    for (auto [nodeId, node] : nodes) {
        if (!IsObjectAlive(node)) {
            continue;
        }

        OnNodeUpdated(node);
    }
}

void TChunkPlacement::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    IsDataCenterAware_ = GetDynamicConfig()->UseDataCenterAwareReplicator;

    RecomputeDataCenterSets();
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    RegisterNode(node);
}

void TChunkPlacement::RegisterNode(TNode* node)
{
    if (!node->ReportedDataNodeHeartbeat()) {
        return;
    }

    InsertToLoadFactorMaps(node);
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    UnregisterNode(node);
    RegisterNode(node);
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    UnregisterNode(node);
}

void TChunkPlacement::UnregisterNode(TNode* node)
{
    node->ClearSessionHints();

    RemoveFromLoadFactorMaps(node);
}

void TChunkPlacement::OnNodeDisposed(TNode* node)
{
    for (const auto& item : node->LoadFactorIterators()) {
        YT_VERIFY(!item.second);
    }
}

void TChunkPlacement::OnDataCenterChanged(TDataCenter* /*dataCenter*/)
{
    RecomputeDataCenterSets();
}

bool TChunkPlacement::IsDataCenterFeasible(const TDataCenter* dataCenter) const
{
    return AliveStorageDataCenters_.contains(dataCenter);
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    const std::optional<TString>& preferredHostName,
    ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        /*replicaIndexes*/ {},
        desiredCount,
        minCount,
        /*forceRackAwareness*/ sessionType == ESessionType::Replication,
        replicationFactorOverride,
        forbiddenNodes,
        allocatedNodes,
        preferredHostName);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

TNodeList TChunkPlacement::GetConsistentPlacementWriteTargets(const TChunk* chunk, int mediumIndex)
{
    YT_ASSERT(IsConsistentChunkPlacementEnabled());
    YT_VERIFY(chunk->HasConsistentReplicaPlacementHash());
    return ConsistentPlacement_->GetWriteTargets(chunk, mediumIndex);
}

void TChunkPlacement::InsertToLoadFactorMaps(TNode* node)
{
    RemoveFromLoadFactorMaps(node);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto chunkHostMasterCellCount = multicellManager->GetRoleMasterCellCount(EMasterCellRole::ChunkHost);

    // Iterate through IOWeights because IsValidWriteTargetToInsert check if IOWeights contains medium.
    for (const auto& [mediumIndex, _] : node->IOWeights()) {
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);

        if (!IsValidWriteTargetToInsert(medium, node)) {
            continue;
        }

        auto loadFactor = node->GetLoadFactor(mediumIndex, chunkHostMasterCellCount);
        if (!loadFactor) {
            continue;
        }

        auto it = MediumToLoadFactorToNode_[medium].emplace(*loadFactor, node);
        node->SetLoadFactorIterator(mediumIndex, it);
    }
}

void TChunkPlacement::RemoveFromLoadFactorMaps(TNode* node)
{
    for (const auto& [mediumIndex, factorMapIter] : node->LoadFactorIterators()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!factorMapIter || !medium) {
            continue;
        }

        auto mediumToFactorMapIter = MediumToLoadFactorToNode_.find(medium);
        YT_VERIFY(mediumToFactorMapIter != MediumToLoadFactorToNode_.end());

        auto& factorMap = mediumToFactorMapIter->second;
        factorMap.erase(*factorMapIter);
        node->SetLoadFactorIterator(mediumIndex, std::nullopt);

        if (factorMap.empty()) {
            MediumToLoadFactorToNode_.erase(mediumToFactorMapIter);
        }
    }
}

TNodeList TChunkPlacement::GetWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    bool forceRackAwareness,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    const std::optional<TString>& preferredHostName,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica)
{
    auto* preferredNode = FindPreferredNode(preferredHostName, medium);

    auto consistentPlacementWriteTargets = FindConsistentPlacementWriteTargets(
        medium,
        chunk,
        replicaIndexes,
        desiredCount,
        minCount,
        forbiddenNodes,
        allocatedNodes,
        preferredNode);

    // We may have trouble placing replicas consistently. In that case, ignore
    // CRP for the time being.
    // This may happen when:
    //   - #forbiddenNodes are specified (which means a writer already has trouble);
    //   - a target node dictated by CRP is unavailable (and more time is required
    //     by CRP to react to that);
    //   - etc.
    // In any such case we rely on the replicator to do its job later.
    if (consistentPlacementWriteTargets) {
        return *consistentPlacementWriteTargets;
    }

    const TLoadFactorToNodeMap* loadFactorToNodeMap = nullptr;

    if (auto it = MediumToLoadFactorToNode_.find(medium);
        it == MediumToLoadFactorToNode_.end())
    {
        return TNodeList();
    } else {
        loadFactorToNodeMap = &it->second;
    }

    TTargetCollector collector(
        this,
        medium,
        chunk,
        replicationFactorOverride,
        Config_->AllowMultipleErasurePartsPerNode && chunk->IsErasure(),
        forbiddenNodes,
        allocatedNodes,
        unsafelyPlacedReplica);

    auto tryAdd = [&] (TNode* node, bool enableRackAwareness, bool enableDataCenterAwareness) {
        if (!IsValidWriteTargetToAllocate(
            node,
            &collector,
            enableRackAwareness,
            enableDataCenterAwareness))
        {
            return false;
        }
        collector.AddNode(node);
        return true;
    };

    auto hasEnoughTargets = [&] {
        return std::ssize(collector.GetAddedNodes()) == desiredCount;
    };

    auto loadFactorToNodeIterator = loadFactorToNodeMap->begin();

    auto tryAddAll = [&] (bool enableRackAwareness, bool enableDataCenterAwareness) {
        YT_VERIFY(!hasEnoughTargets());

        bool hasProgress = false;
        if (loadFactorToNodeIterator == loadFactorToNodeMap->end()) {
            loadFactorToNodeIterator = loadFactorToNodeMap->begin();
        }

        for ( ; !hasEnoughTargets() && loadFactorToNodeIterator != loadFactorToNodeMap->end(); ++loadFactorToNodeIterator) {
            auto* node = loadFactorToNodeIterator->second;
            hasProgress |= tryAdd(node, enableRackAwareness, enableDataCenterAwareness);
        }
        return hasProgress;
    };

    if (preferredNode) {
        tryAdd(
            preferredNode,
            /*enableRackAwareness*/ true,
            /*enableDataCenterAwareness*/ IsDataCenterAware_);
    }

    if (!hasEnoughTargets()) {
        tryAddAll(/*enableRackAwareness*/ true, /*enableDataCenterAwareness*/ IsDataCenterAware_);
    }

    if (!forceRackAwareness) {
        while (!hasEnoughTargets()) {
            // Disabling rack awareness also disables data center awareness.
            if (!tryAddAll(/*enableRackAwareness*/ false, /*enableDataCenterAwareness*/ false)) {
                break;
            }
            if (!chunk->IsErasure() || !Config_->AllowMultipleErasurePartsPerNode) {
                break;
            }
        }
    }

    const auto& nodes = collector.GetAddedNodes();
    return std::ssize(nodes) < minCount ? TNodeList() : nodes;
}

TNode* TChunkPlacement::FindPreferredNode(
    const std::optional<TString>& preferredHostName,
    TMedium* medium)
{
    if (!preferredHostName) {
        return nullptr;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto* preferredHost = nodeTracker->FindHostByName(*preferredHostName);
    // COMPAT(gritukan)
    if (!preferredHost) {
        if (auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName)) {
            preferredHost = preferredNode->GetHost();
        }
    }

    if (!preferredHost) {
        return nullptr;
    }

    for (auto* node : preferredHost->GetNodesWithFlavor(ENodeFlavor::Data)) {
        if (IsValidPreferredWriteTargetToAllocate(node, medium)) {
            // NB: assuming a single data node per host here.
            return node;
        }
    }

    return nullptr;
}

std::optional<TNodeList> TChunkPlacement::FindConsistentPlacementWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    TNode* preferredNode)
{
    YT_ASSERT(replicaIndexes.empty() || std::ssize(replicaIndexes) == minCount);
    YT_ASSERT(std::find(replicaIndexes.begin(), replicaIndexes.end(), GenericChunkReplicaIndex) == replicaIndexes.end());
    YT_ASSERT(replicaIndexes.empty() || chunk->IsErasure());

    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return std::nullopt;
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return std::nullopt;
    }

    auto mediumIndex = medium->GetIndex();
    auto result = GetConsistentPlacementWriteTargets(chunk, mediumIndex);

    if (result.empty()) {
        return std::nullopt; // No online nodes.
    }

    if (minCount > std::ssize(result) || desiredCount > std::ssize(result)) {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& dataNodeStatistics = nodeTracker->GetFlavoredNodeStatistics(ENodeFlavor::Data);
        if (desiredCount > dataNodeStatistics.OnlineNodeCount) {
            YT_LOG_WARNING("Requested to allocate too many consistently placed chunk replica targets "
                "(ChunkId: %v, ReplicaIndexes: %v, MediumIndex: %v, MinReplicaCount: %v, DesiredReplicaCount: %v, ConsistentPlacementReplicaCount: %v, OnlineDataNodeCount: %v)",
                chunk->GetId(),
                replicaIndexes,
                mediumIndex,
                minCount,
                desiredCount,
                std::ssize(result),
                dataNodeStatistics.OnlineNodeCount);
        }
        return std::nullopt;
    }

    // NB: replicaIndexes may be empty.
    if (std::find_if(
        replicaIndexes.begin(),
        replicaIndexes.end(),
        [&] (int replicaIndex) {
            return replicaIndex >= std::ssize(result);
        })!= replicaIndexes.end())
    {
        YT_LOG_ALERT("Target nodes dictated by consistent chunk placement are fewer than the specified replica index (ChunkId: %v, MediumIndex: %v, ConsistentPlacementTargetNodeCount: %v, ReplicaIndexes: %v)",
            chunk->GetId(),
            mediumIndex,
            std::ssize(result),
            replicaIndexes);
        return std::nullopt;
    }

    if (!replicaIndexes.empty()) {
        TNodeList filteredResult;
        filteredResult.reserve(replicaIndexes.size());
        for (auto replicaIndex : replicaIndexes) {
            filteredResult.push_back(result[replicaIndex]);
        }
        result = std::move(filteredResult);
        YT_ASSERT(std::ssize(replicaIndexes) == std::ssize(result));
    }

    YT_ASSERT(std::all_of(
        result.begin(),
        result.end(),
        [&] (TNode* node) {
            return node->IsValidWriteTarget();
        }));

    auto isNodeForbidden = [&] (TNode* node) {
        if (forbiddenNodes &&
            std::find(forbiddenNodes->begin(), forbiddenNodes->end(), node) != forbiddenNodes->end())
        {
            return true;
        }

        if (allocatedNodes &&
            std::find(allocatedNodes->begin(), allocatedNodes->end(), node) != allocatedNodes->end())
        {
            return true;
        }

        return false;
    };

    auto isNodeConsistent = [&] (TNode* node, int replicaIndex) {
        for (auto replica : chunk->StoredReplicas()) {
            if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
                continue;
            }

            if (replicaIndex == GenericChunkReplicaIndex) {
                if (GetChunkLocationNode(replica) == node) {
                    return true;
                }
            } else if (replica.GetReplicaIndex() == replicaIndex) {
                return GetChunkLocationNode(replica) == node;
            }
        }

        return false;
    };

    // Regular and erasure chunks are fundamentally different: for the former,
    // it's ok to reorder replicas and therefore we're allowed to filter out
    // some target nodes if necessary. For erasure chunks, a need to filter a
    // target node out means failing to place replicas consistently.

    // NB: the code below is quadratic, but all factors are small.
    if (chunk->IsErasure()) {
        for (auto* node : result) {
            if (isNodeForbidden(node)) {
                return std::nullopt;
            }

            if (replicaIndexes.empty()) {
                for (auto replicaIndex = 0; replicaIndex < std::ssize(result); ++replicaIndex) {
                    auto* node = result[replicaIndex];
                    if (isNodeConsistent(node, replicaIndex)) {
                        return std::nullopt;
                    }
                }
            } else {
                for (auto i = 0; i < std::ssize(result); ++i) {
                    auto* node = result[i];
                    auto replicaIndex = replicaIndexes[i];
                    if (isNodeConsistent(node, replicaIndex)) {
                        return std::nullopt;
                    }
                }
            }
        }
    } else {
        result.erase(
            std::remove_if(
                result.begin(),
                result.end(),
                [&] (TNode* node) {
                    return isNodeForbidden(node) || isNodeConsistent(node, GenericChunkReplicaIndex);
                }),
            result.end());
    }

    if (std::ssize(result) < minCount) {
        return std::nullopt;
    }

    YT_VERIFY(!result.empty());

    YT_ASSERT(desiredCount >= std::ssize(replicaIndexes));
    if (desiredCount < std::ssize(result)) {
        // Make sure the preferred node makes it to the result after trimming.
        if (preferredNode && !chunk->IsErasure()) {
            auto it = std::find(result.begin(), result.end(), preferredNode);
            if (it != result.end()) {
                std::swap(result.front(), *it);
            }
        }
        // Trim the result.
        auto tailIt = result.begin();
        std::advance(tailIt, desiredCount);
        result.erase(tailIt, result.end());
    }

   return result;
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    ESessionType sessionType,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        replicaIndexes,
        desiredCount,
        minCount,
        sessionType == ESessionType::Replication,
        replicationFactorOverride,
        /*forbiddenNodes*/ nullptr,
        /*allocatedNodes*/ nullptr,
        /*preferredHostName*/ std::nullopt,
        unsafelyPlacedReplica);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

TChunkLocation* TChunkPlacement::GetRemovalTarget(TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto mediumIndex = chunkWithIndexes.GetMediumIndex();
    auto maxReplicasPerRack = GetMaxReplicasPerRack(mediumIndex, chunk);

    std::array<i8, RackIndexBound> perRackCounters{};
    // TODO(gritukan): YT-16557.
    TCompactFlatMap<const TDataCenter*, i8, 4> perDataCenterCounters;

    for (auto replica : chunk->StoredReplicas()) {
        if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
            continue;
        }

        if (const auto* rack = GetChunkLocationNode(replica)->GetRack()) {
            ++perRackCounters[rack->GetIndex()];
            if (const auto* dataCenter = rack->GetDataCenter()) {
                ++perDataCenterCounters[dataCenter];
            }
        }
    }

    // An arbitrary node that violates consistent placement requirements.
    TChunkLocation* consistentPlacementWinner = nullptr;
    // An arbitrary node from a rack with too many replicas.
    TChunkLocation* rackWinner = nullptr;
    // An arbitrary node from a data center with too many replicas.
    TChunkLocation* dataCenterWinner = nullptr;
    // A node with the largest fill factor.
    TChunkLocation* fillFactorWinner = nullptr;

    TNodeList consistentPlacementNodes;
    if (chunk->HasConsistentReplicaPlacementHash() && IsConsistentChunkPlacementEnabled()) {
        // NB: Do not ask for consistent chunk placement on unexpected medium.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        const auto& replication = chunk->GetAggregatedReplication(requisitionRegistry);
        for (const auto& entry : replication) {
            if (entry.GetMediumIndex() == mediumIndex) {
                consistentPlacementNodes = GetConsistentPlacementWriteTargets(chunk, mediumIndex);
                break;
            }
        }
    }

    auto isInconsistentlyPlaced = [&] (TNode* node) {
        if (!chunk->HasConsistentReplicaPlacementHash()) {
            return false;
        }

        if (!IsConsistentChunkPlacementEnabled()) {
            return false;
        }

        if (consistentPlacementNodes.empty()) {
            return false; // No online nodes.
        }

        return replicaIndex == GenericChunkReplicaIndex
            ? std::find(consistentPlacementNodes.begin(), consistentPlacementNodes.end(), node) == consistentPlacementNodes.end()
            : consistentPlacementNodes[replicaIndex] != node;
    };

    for (auto replica : chunk->StoredReplicas()) {
        if (chunk->IsJournal() && replica.GetReplicaState() != EChunkReplicaState::Sealed) {
            continue;
        }

        if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
            continue;
        }

        if (replica.GetReplicaIndex() != replicaIndex) {
            continue;
        }

        auto* location = replica.GetPtr();
        auto* node = location->GetNode();
        if (!IsValidRemovalTarget(node)) {
            continue;
        }

        if (isInconsistentlyPlaced(node)) {
            consistentPlacementWinner = location;
        }

        if (const auto* rack = node->GetRack()) {
            if (perRackCounters[rack->GetIndex()] > maxReplicasPerRack) {
                rackWinner = location;
            }

            if (const auto* dataCenter = rack->GetDataCenter()) {
                auto maxReplicasPerDataCenter = GetMaxReplicasPerDataCenter(mediumIndex, chunk, dataCenter);
                if (perDataCenterCounters[dataCenter] > maxReplicasPerDataCenter) {
                    dataCenterWinner = location;
                }
            }
        }

        auto nodeFillFactor = node->GetFillFactor(mediumIndex);

        if (nodeFillFactor &&
            (!fillFactorWinner ||
                *nodeFillFactor > *fillFactorWinner->GetNode()->GetFillFactor(mediumIndex)))
        {
            fillFactorWinner = location;
        }
    }

    if (consistentPlacementWinner) {
        return consistentPlacementWinner;
    } else if (rackWinner) {
        return rackWinner;
    } else if (dataCenterWinner) {
        return dataCenterWinner;
    } else {
        return fillFactorWinner;
    }
}

bool TChunkPlacement::IsValidWriteTargetToInsert(TMedium* medium, TNode* node)
{
    if (!node->IsWriteEnabled(medium->GetIndex())) {
        // Do not write anything to nodes not accepting writes.
        return false;
    }

    return IsValidWriteTargetCore(node);
}

bool TChunkPlacement::IsValidPreferredWriteTargetToAllocate(TNode* node, TMedium* medium)
{
    if (!node->IsWriteEnabled(medium->GetIndex())) {
        return false;
    }

    return true;
}

bool TChunkPlacement::IsValidWriteTargetToAllocate(
    TNode* node,
    TTargetCollector* collector,
    bool enableRackAwareness,
    bool enableDataCenterAwareness)
{
    // Check node first.
    if (!IsValidWriteTargetCore(node)) {
        return false;
    }

    // If replicator is data center aware, unaware nodes are not allowed.
    if (enableDataCenterAwareness && !node->GetDataCenter()) {
        return false;
    }

    if (!collector->CheckNode(node, enableRackAwareness, enableDataCenterAwareness)) {
        // The collector does not like this node.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidWriteTargetCore(TNode* node)
{
    if (!node->IsValidWriteTarget()) {
        return false;
    }

    // The above only checks DisableWriteSessions, not Effective*.
    if (node->GetEffectiveDisableWriteSessions()) {
        return false;
    }

    if (IsDataCenterAware_) {
        const auto* dataCenter = node->GetDataCenter();
        if (!dataCenter || !IsDataCenterFeasible(dataCenter)) {
            return false;
        }
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidRemovalTarget(TNode* node)
{
    if (!node->ReportedDataNodeHeartbeat()) {
        // Do not remove anything from a node before its first heartbeat or after it is unregistered.
        return false;
    }

    return true;
}

void TChunkPlacement::AddSessionHint(TNode* node, int mediumIndex, ESessionType sessionType)
{
    node->AddSessionHint(mediumIndex, sessionType);

    RemoveFromLoadFactorMaps(node);
    InsertToLoadFactorMaps(node);
}

int TChunkPlacement::GetMaxReplicasPerRack(
    const TMedium* medium,
    const TChunk* chunk,
    std::optional<int> /*replicationFactorOverride*/) const
{
    //auto result = chunk->GetMaxReplicasPerFailureDomain(
    //    medium->GetIndex(),
    //    replicationFactorOverride,
    //    Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry());
    //result = std::min(result, config->MaxReplicasPerRack);
    const auto& config = medium->Config();
    auto result = config->MaxReplicasPerRack;

    switch (chunk->GetType()) {
        case EObjectType::Chunk:
            result = std::min(result, config->MaxRegularReplicasPerRack);
            break;
        case EObjectType::ErasureChunk:
            result = std::min(result, config->MaxErasureReplicasPerRack);
            break;
        case EObjectType::JournalChunk:
            result = std::min(result, config->MaxJournalReplicasPerRack);
            break;
        case EObjectType::ErasureJournalChunk:
            result = std::min(result, config->MaxErasureJournalReplicasPerRack);
            break;
        default:
            YT_ABORT();
    }
    return result;
}

int TChunkPlacement::GetMaxReplicasPerRack(
    int mediumIndex,
    const TChunk* chunk,
    std::optional<int> replicationFactorOverride) const
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
    return GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride);
}

int TChunkPlacement::GetMaxReplicasPerDataCenter(
    const TMedium* medium,
    const TChunk* chunk,
    const TDataCenter* dataCenter,
    std::optional<int> replicationFactorOverride) const
{
    return GetMaxReplicasPerDataCenter(medium->GetIndex(), chunk, dataCenter, replicationFactorOverride);
}

int TChunkPlacement::GetMaxReplicasPerDataCenter(
    int mediumIndex,
    const TChunk* chunk,
    const TDataCenter* dataCenter,
    std::optional<int> replicationFactorOverride) const
{
    if (!IsDataCenterAware_) {
        return Max<int>();
    }

    if (!IsDataCenterFeasible(dataCenter)) {
        return 0;
    }

    auto* chunkRequisitionRegistry = Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry();
    auto replicaCount = replicationFactorOverride.value_or(
        chunk->GetPhysicalReplicationFactor(mediumIndex, chunkRequisitionRegistry));
    auto aliveStorageDataCenterCount = std::ssize(AliveStorageDataCenters_);
    if (aliveStorageDataCenterCount == 0) {
        // Dividing by zero is bad, so case of zero alive data centers is handled separately.
        // Actually, in this case replica allocation is impossible, so we can return any possible value.
        return replicaCount;
    }

    auto maxReplicasPerDataCenter = DivCeil<int>(replicaCount, aliveStorageDataCenterCount);
    auto maxReplicasPerFailureDomain = chunk->GetMaxReplicasPerFailureDomain(
        mediumIndex,
        replicationFactorOverride,
        chunkRequisitionRegistry);

    // Typically it's impossible to store chunk in such a way that after data center loss it is still
    // available when one data center is already banned, so we do not consider data center as a failure
    // domain when there are banned data centers.
    // Consider a cluster with 3 data centers and chunk with erasure codec RS(6, 3). When one data center
    // is lost, at least one data center will store at least 5 of its replicas which is too much to repair
    // chunk from the rest parts.
    if (BannedStorageDataCenters_.empty()) {
        maxReplicasPerDataCenter = std::min<int>(maxReplicasPerDataCenter, maxReplicasPerFailureDomain);
    }

    return maxReplicasPerDataCenter;
}

const std::vector<TError>& TChunkPlacement::GetAlerts() const
{
    return DataCenterSetErrors_;
}

const TDynamicChunkManagerConfigPtr& TChunkPlacement::GetDynamicConfig() const
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

bool TChunkPlacement::IsConsistentChunkPlacementEnabled() const
{
    return GetDynamicConfig()->ConsistentReplicaPlacement->Enable;
}

void TChunkPlacement::RecomputeDataCenterSets()
{
    // At first, clear everything.
    auto oldStorageDataCenters = std::move(StorageDataCenters_);
    auto oldBannedStorageDataCenters = std::move(BannedStorageDataCenters_);
    auto oldAliveStorageDataCenters = std::move(AliveStorageDataCenters_);
    DataCenterSetErrors_.clear();

    auto refreshGuard = Finally([&] {
        if (StorageDataCenters_ != oldStorageDataCenters ||
            BannedStorageDataCenters_ != oldBannedStorageDataCenters ||
            AliveStorageDataCenters_ != oldAliveStorageDataCenters)
        {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ScheduleGlobalChunkRefresh();
        }
    });

    // If replicator is not data center aware, data center sets are not required.
    if (!IsDataCenterAware_) {
        return;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& storageDataCenter : GetDynamicConfig()->StorageDataCenters) {
        if (auto* dataCenter = nodeTracker->FindDataCenterByName(storageDataCenter); IsObjectAlive(dataCenter)) {
            InsertOrCrash(StorageDataCenters_, dataCenter);
        } else {
            auto error = TError("Storage data center %Qv is unknown",
                storageDataCenter);
            DataCenterSetErrors_.push_back(error);
        }
    }

    for (const auto& bannedDataCenter : GetDynamicConfig()->BannedStorageDataCenters) {
        if (auto* dataCenter = nodeTracker->FindDataCenterByName(bannedDataCenter); IsObjectAlive(dataCenter)) {
            if (StorageDataCenters_.contains(dataCenter)) {
                InsertOrCrash(BannedStorageDataCenters_, dataCenter);
            } else {
                auto error = TError("Banned data center %Qv is not a storage data center",
                    bannedDataCenter);
                DataCenterSetErrors_.push_back(error);
            }
        } else {
            auto error = TError("Banned data center %Qv is unknown",
                bannedDataCenter);
            DataCenterSetErrors_.push_back(error);
        }
    }

    for (auto* dataCenter : StorageDataCenters_) {
        if (!BannedStorageDataCenters_.contains(dataCenter)) {
            InsertOrCrash(AliveStorageDataCenters_, dataCenter);
        }
    }

    THashSet<const TDataCenter*> livenessChangedDataCenters;
    for (auto* dataCenter : AliveStorageDataCenters_) {
        if (!oldAliveStorageDataCenters.contains(dataCenter)) {
            livenessChangedDataCenters.insert(dataCenter);
        }
    }
    for (auto* dataCenter : oldAliveStorageDataCenters) {
        if (!AliveStorageDataCenters_.contains(dataCenter)) {
            livenessChangedDataCenters.insert(dataCenter);
        }
    }
    for (auto* dataCenter : livenessChangedDataCenters) {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto* rack : nodeTracker->GetDataCenterRacks(dataCenter)) {
            for (auto* node : nodeTracker->GetRackNodes(rack)) {
                OnNodeUpdated(node);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
