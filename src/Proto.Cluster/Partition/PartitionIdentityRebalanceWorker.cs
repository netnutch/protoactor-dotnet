// -----------------------------------------------------------------------
// <copyright file="TopologyRelocationActor.cs" company="Asynkron AB">
//      Copyright (C) 2015-2021 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Partition
{
    /// <summary>
    /// Used by partitionIdentityActor to update partition lookup on topology changes.
    /// Delegates each member to a separate worker actor which handles chunked transfers and can be retried on a member basis.
    /// </summary>
    class PartitionIdentityRebalanceWorker : IActor, IDisposable
    {
        private readonly TimeSpan _handoverTimeout;
        private readonly CancellationToken _cancellationToken;
        private static readonly ILogger Logger = Log.CreateLogger<PartitionIdentityActor>();

        private readonly Dictionary<ClusterIdentity, PID> _partitionLookup;
        private readonly TaskCompletionSource<bool> _onRelocationComplete;
        private readonly Dictionary<string, PID> _waitingRequests = new();
        private readonly Stopwatch _timer = new();
        private IdentityHandoverRequest? _request;
        private CancellationTokenRegistration? _tokenRegistration;

        public PartitionIdentityRebalanceWorker(int guesstimatedActivationCount, TimeSpan handoverTimeout, CancellationToken cancellationToken)
        {
            _handoverTimeout = handoverTimeout;
            _cancellationToken = cancellationToken;
            _partitionLookup = new Dictionary<ClusterIdentity, PID>(guesstimatedActivationCount);
            _onRelocationComplete = new TaskCompletionSource<bool>();
        }

        public Task ReceiveAsync(IContext context) => context.Message switch
        {
            IdentityHandoverRequest request             => OnIdentityHandoverRequest(request, context),
            PartitionWorker.PartitionCompleted response => OnPartitionCompleted(response, context),
            PartitionWorker.PartitionFailed response    => OnPartitionFailed(response, context),
            _                                           => Task.CompletedTask
        };

        private Task OnIdentityHandoverRequest(IdentityHandoverRequest request, IContext context)
        {
            _tokenRegistration = _cancellationToken.Register(() => context.Self.Stop(context.System)
            );
            _timer.Start();
            _request = request;

            foreach (var member in request.Members)
            {
                var memberAddress = member.Address;
                StartRebalanceFromMember(request, context, memberAddress);
            }

            context.ReenterAfter(_onRelocationComplete.Task, () => {
                    context.Respond(new PartitionsRebalanced(_partitionLookup, _request.TopologyHash));
                    context.Self.Stop(context.System);
                }
            );
            return Task.CompletedTask;
        }

        private Task OnPartitionCompleted(PartitionWorker.PartitionCompleted response, IContext context)
        {
            foreach (var activation in response.Activations)
            {
                TakeOwnership(activation, context);
            }

            _waitingRequests.Remove(response.MemberAddress);

            if (_waitingRequests.Count == 0)
            {
                _timer.Stop();
                Logger.LogDebug("IdentityRelocation completed, received {Count} actors in {Elapsed}", _partitionLookup.Count, _timer.Elapsed);
                _onRelocationComplete.TrySetResult(true);
            }

            return Task.CompletedTask;
        }

        private Task OnPartitionFailed(PartitionWorker.PartitionFailed response, IContext context)
        {
            Logger.LogWarning("Retrying member {Member}, failed with {Reason}", response.MemberAddress, response.Reason);
            StartRebalanceFromMember(_request!, context, response.MemberAddress);
            return Task.CompletedTask;
        }

        private void TakeOwnership(Activation msg, IContext context)
        {
            if (_partitionLookup.TryGetValue(msg.ClusterIdentity, out var existing))
            {
                if (existing.Address == msg.Pid.Address && existing.Id == msg.Pid.Id) return; // Identical activation, no-op

                // If they are not equal, we might have multiple activations, ie invalid state
                Logger.LogWarning("Duplicate activation: {ClusterIdentity}, {Pid1}, {Pid2}", msg.ClusterIdentity, existing, msg.Pid);
                context.Poison(existing);
            }

            // Logger.LogDebug("Taking Ownership of: {Identity}, pid: {Pid}", msg.Identity, msg.Pid);
            _partitionLookup[msg.ClusterIdentity] = msg.Pid;
        }

        private void StartRebalanceFromMember(IdentityHandoverRequest request, IContext context, string memberAddress)
        {
            var childPid = context.Spawn(Props.FromProducer(() => new PartitionWorker(memberAddress, _handoverTimeout)));
            context.Request(childPid, request);
            _waitingRequests[memberAddress] = childPid;
        }

        /// <summary>
        /// Handles a single member rebalance.
        /// Split out to make sure a failure against a single member can be retried without affecting the rest.
        /// </summary>
        private class PartitionWorker : IActor
        {
            private readonly PID _targetMember;
            private readonly HashSet<int> _receivedChunks = new();
            private readonly List<Activation> _receivedActivations = new();
            private int? _finalChunk;
            private readonly string _memberAddress;
            private readonly TimeSpan _timeout;
            private readonly TaskCompletionSource<object> _completionSource = new();

            public PartitionWorker(string memberAddress, TimeSpan timeout)
            {
                _memberAddress = memberAddress;
                _timeout = timeout;
                _targetMember = PartitionManager.RemotePartitionPlacementActor(_memberAddress);
            }

            public Task ReceiveAsync(IContext context)
            {
                switch (context.Message)
                {
                    case IdentityHandoverRequest msg:
                        OnIdentityHandoverRequest(msg, context);
                        break;
                    case IdentityHandoverResponse msg:
                        OnIdentityHandoverResponse(msg, context);
                        break;
                    case ReceiveTimeout:
                        FailPartition(context, "Timeout");
                        break;
                    case DeadLetterResponse:
                        FailPartition(context, "DeadLetter");
                        break;
                }

                return Task.CompletedTask;
            }

            private void OnIdentityHandoverRequest(IdentityHandoverRequest msg, IContext context)
            {
                context.Request(_targetMember, msg);
                context.SetReceiveTimeout(_timeout);
                context.ReenterAfter(_completionSource.Task, () => {
                        context.Send(context.Parent!, _completionSource.Task.Result);
                        context.Stop(context.Self);
                    }
                );
            }

            private void OnIdentityHandoverResponse(IdentityHandoverResponse response, IContext context)
            {
                var sender = context.Sender;

                if (sender is null)
                {
                    // Invalid response, requires sender to be populated
                    Logger.LogError("Invalid IdentityHandoverResponse received, missing sender");
                    FailPartition(context, "MissingSender");
                }

                _receivedActivations.AddRange(response.Actors);

                if (HasReceivedAllChunks(response))
                {
                    _completionSource.SetResult(new PartitionCompleted(_memberAddress, _receivedActivations));
                }
            }

            private void FailPartition(IContext context, string reason)
                => _completionSource.TrySetResult(new PartitionFailed(_memberAddress, reason));

            private bool HasReceivedAllChunks(IdentityHandoverResponse response)
            {
                if (_receivedChunks.Contains(response.ChunkId))
                {
                    Logger.LogWarning("Chunk {ChunkId} already received", response.ChunkId);
                    return false;
                }

                _receivedChunks.Add(response.ChunkId);

                if (response.Final)
                {
                    _finalChunk = response.ChunkId;
                }

                return _finalChunk.HasValue && _receivedChunks.Count == _finalChunk;
            }

            public record PartitionCompleted(string MemberAddress, List<Activation> Activations);

            public record PartitionFailed(string MemberAddress, string Reason);
        }

        public void Dispose() => _tokenRegistration?.Dispose();
    }
}