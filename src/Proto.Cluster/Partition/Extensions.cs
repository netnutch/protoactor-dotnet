// -----------------------------------------------------------------------
// <copyright file="Extensions.cs" company="Asynkron AB">
//      Copyright (C) 2015-2021 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using Proto.Cluster.Gossip;

namespace Proto.Cluster.Partition
{
    internal static class Extensions
    {
        private const string HandoverStateKey = "reb:ready";

        public static async Task<(bool consensus, ulong topologyHash)> WaitUntilInFlightActivationsAreCompleted(
            this Gossiper gossip,
            CancellationToken cancellationToken
        )
        {
            using var consensusCheck = gossip.RegisterConsensusCheck<ReadyForRebalance, ulong>(
                    HandoverStateKey,
                    rebalance => rebalance.TopologyHash
                );

            return await consensusCheck.GossipConsensus(cancellationToken);
        }

        public static void SetInFlightActivationsCompleted(this Gossiper gossip, ulong topologyHash)
            => gossip.SetState(HandoverStateKey, new ReadyForRebalance {TopologyHash = topologyHash});
    }
}