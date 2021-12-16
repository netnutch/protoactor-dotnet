// -----------------------------------------------------------------------
// <copyright file="Extensions.cs" company="Asynkron AB">
//      Copyright (C) 2015-2021 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

namespace Proto.Cluster.Partition
{
    internal static class Extensions
    {
        private const string HandoverStateKey = "reb:ready";

        public static async Task<(bool consensus, ulong topologyHash)> WaitUntilInFlightActivationsAreCompleted(
            this Cluster cluster,
            CancellationToken cancellationToken
        )
        {
            await using var consensusCheck =
                await cluster.Gossip.RegisterConsensusCheck<ReadyForRebalance, ulong>(
                    HandoverStateKey,
                    rebalance => rebalance.TopologyHash
                );

            return await consensusCheck.GossipConsensus(cancellationToken);
        }

        public static void SetInFlightActivationsCompleted(this Cluster cluster, ulong topologyHash)
            => cluster.Gossip.SetState(HandoverStateKey, new ReadyForRebalance {TopologyHash = topologyHash});
    }
}