// -----------------------------------------------------------------------
// <copyright file="PartitionIdentityActor.cs" company="Asynkron AB">
//      Copyright (C) 2015-2020 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto.Cluster.Gossip;

namespace Proto.Cluster.Partition
{
    //This actor is responsible to keep track of identities owned by this member
    //it does not manage the cluster spawned actors itself, only identity->remote PID management
    //TLDR; this is a partition/bucket in the distributed hash table which makes up the identity lookup
    //
    //for spawning/activating cluster actors see PartitionActivator.cs
    class PartitionIdentityActor : IActor
    {
        //for how long do we wait when performing a identity handover?

        private readonly Cluster _cluster;
        private static readonly ILogger Logger = Log.CreateLogger<PartitionIdentityActor>();
        private readonly string _myAddress;

        private readonly Dictionary<ClusterIdentity, PID> _partitionLookup = new(); //actor/grain name to PID

        private MemberHashRing _memberHashRing = new(ImmutableList<Member>.Empty);

        private readonly Dictionary<ClusterIdentity, Task<ActivationResponse>> _spawns = new();

        private ulong _topologyHash;
        private readonly TimeSpan _identityHandoverTimeout;
        private readonly PartitionConfig _config;

        private TaskCompletionSource<ulong>? _rebalanceTcs;
        private CancellationTokenSource? _rebalanceCancellation;
        private Task<(bool consensus, ulong topologyHash)>? _waitForInflightActivations;

        public PartitionIdentityActor(Cluster cluster, TimeSpan identityHandoverTimeout, PartitionConfig config)
        {
            _cluster = cluster;
            _myAddress = cluster.System.Address;
            _identityHandoverTimeout = identityHandoverTimeout;
            _config = config;
        }

        public Task ReceiveAsync(IContext context) =>
            context.Message switch
            {
                Started                  => OnStarted(context),
                ActivationRequest msg    => OnActivationRequest(msg, context),
                ActivationTerminated msg => OnActivationTerminated(msg),
                ClusterTopology msg      => OnClusterTopology(msg, context),
                _                        => Task.CompletedTask
            };

        private Task OnStarted(IContext context)
        {
            var self = context.Self;
            _cluster.System.EventStream.Subscribe<ActivationTerminated>(e => _cluster.System.Root.Send(self, e));

            return Task.CompletedTask;
        }

        private Task OnClusterTopology(ClusterTopology msg, IContext context)
        {
            if (_topologyHash.Equals(msg.TopologyHash))
            {
                return Task.CompletedTask;
            }

            StopInvalidatedTopologyRebalance(msg);

            var topologyHash = msg.TopologyHash;
            _topologyHash = topologyHash;
            _memberHashRing = new MemberHashRing(msg.Members);
            var existingActivations = _partitionLookup.Count;
            //remove all identities we do no longer own.
            _partitionLookup.Clear();

            if (msg.Members.Count < 1)
            {
                Logger.LogWarning("No active members in cluster topology update");
                return Task.CompletedTask;
            }

            _rebalanceTcs ??= new TaskCompletionSource<ulong>();

            SetReadyToRebalanceIfNoMoreWaitingSpawns();

            if (_waitForInflightActivations is null)
            {
                var timer = Stopwatch.StartNew();
                _waitForInflightActivations = _cluster.Gossip.WaitUntilInFlightActivationsAreCompleted(CancellationTokens.FromSeconds(3));
                context.ReenterAfter(_waitForInflightActivations, consensusTask => {
                        _waitForInflightActivations = null;
                        timer.Stop();
                        var allNodesCompletedActivations = consensusTask.Result.consensus;

                        if (allNodesCompletedActivations)
                        {
                            Logger.LogDebug("{SystemId} All nodes OK, Initiating rebalance:, {CurrentTopology} {ConsensusHash} after {Duration}",
                                _cluster.System.Id, _topologyHash, consensusTask.Result.topologyHash, timer.Elapsed
                            );
                        }
                        else
                        {
                            Logger.LogError(
                                "{SystemId} Consensus not reached, Initiating rebalance:, {CurrentTopology} {ConsensusHash} after {Duration}",
                                _cluster.System.Id, _topologyHash, consensusTask.Result.topologyHash, timer.Elapsed
                            );
                        }

                        InitRebalance(msg, context, existingActivations);
                        return Task.CompletedTask;
                    }
                );
            }

            return Task.CompletedTask;
        }

        private void SetReadyToRebalanceIfNoMoreWaitingSpawns()
        {
            if (_spawns.Count == 0)
            {
                _cluster.Gossip.SetInFlightActivationsCompleted(_topologyHash);
            }
        }

        private void InitRebalance(ClusterTopology msg, IContext context, int existingActivations)
        {
            Logger.LogDebug("Requesting ownerships");
            _rebalanceCancellation = new CancellationTokenSource();
            var workerPid = SpawnRebalanceWorker(context, existingActivations, _rebalanceCancellation.Token);
            var rebalanceTask = context.RequestAsync<PartitionsRebalanced>(workerPid, new IdentityHandoverRequest
                {
                    TopologyHash = _topologyHash,
                    Address = _myAddress,
                    Members = {msg.Members}
                }, _rebalanceCancellation.Token
            );

            context.ReenterAfter(rebalanceTask, task => {
                    if (task.IsCompletedSuccessfully)
                    {
                        return OnPartitionsRebalanced(task.Result, context);
                    }

                    Logger.LogInformation("Partition Rebalance cancelled for {TopologyHash}", _topologyHash);
                    return Task.CompletedTask;
                }
            );
        }

        private PID SpawnRebalanceWorker(IContext context, int existingActivations, CancellationToken cancellationToken) => context.Spawn(
            Props.FromProducer(()
                => new PartitionIdentityRebalanceWorker((int) (existingActivations * 1.10), _config.RebalanceRequestTimeout,
                    cancellationToken
                )
            )
        );

        private async Task OnPartitionsRebalanced(PartitionsRebalanced msg, IContext context)
        {
            // var (consensus, topologyHash) = await _cluster.MemberList.TopologyConsensus(CancellationToken.None);
            //
            // while (consensus is false)
            // {
            //     Logger.LogWarning("Waiting for consensus to complete partition rebalance");
            //
            //     (consensus, topologyHash) = await _cluster.MemberList.TopologyConsensus(CancellationToken.None);
            // }

            if (msg.TopologyHash != _topologyHash)
            {
                Console.WriteLine($"Rebalance with outdated TopologyHash {msg.TopologyHash}!={_topologyHash}");
                Logger.LogWarning("Rebalance with outdated TopologyHash {Received} instead of {Current}", msg.TopologyHash, _topologyHash);
                return;
            }

            if (_config.DeveloperLogging)
            {
                Console.WriteLine($"Got ownerships {msg.TopologyHash} / {_topologyHash}");
            }

            Logger.LogDebug("Got ownerships {EventId}, {Count}", _topologyHash, msg.OwnedActivations.Count);

            foreach (var activation in msg.OwnedActivations)
            {
                _partitionLookup.Add(activation.Key, activation.Value);
            }

            _rebalanceTcs?.TrySetResult(_topologyHash);
            _rebalanceTcs = null;
        }

        private void StopInvalidatedTopologyRebalance(ClusterTopology msg)
        {
            if (_rebalanceCancellation is not null && _topologyHash != msg.TopologyHash)
            {
                Logger.LogDebug("[PartitionIdentityActor] Cancelling handover of {OldTopology}, new topology is {CurrentTopology}", _topologyHash,
                    msg.TopologyHash
                );

                _rebalanceCancellation.Cancel();
                _rebalanceCancellation = null;
            }
        }

        private Task OnActivationTerminated(ActivationTerminated msg)
        {
            if (_spawns.ContainsKey(msg.ClusterIdentity))
            {
                return Task.CompletedTask;
            }

            //we get this via broadcast to all nodes, remove if we have it, or ignore
            Logger.LogDebug("[PartitionIdentityActor] Terminated {Pid}", msg.Pid);

            if (_partitionLookup.TryGetValue(msg.ClusterIdentity, out var existingActivation) && existingActivation.Equals(msg.Pid))
            {
                _partitionLookup.Remove(msg.ClusterIdentity);
            }

            _cluster.PidCache.RemoveByVal(msg.ClusterIdentity, msg.Pid);

            return Task.CompletedTask;
        }

        private Task OnActivationRequest(ActivationRequest msg, IContext context)
        {
            // Wait for rebalance in progress
            if (_rebalanceTcs is not null)
            {
                if (_config.DeveloperLogging)
                    Console.WriteLine($"Rebalance in progress,  {msg.RequestId}");
                context.ReenterAfter(_rebalanceTcs.Task, _ => OnActivationRequest(msg, context));
                return Task.CompletedTask;
            }

            if (_memberHashRing.Count == 0)
            {
                if (_config.DeveloperLogging)
                    Console.WriteLine($"No active members, {msg.RequestId}");
                RespondWithFailure(context);
                return Task.CompletedTask;
            }

            if (_config.DeveloperLogging)
                Console.WriteLine($"Got ActivationRequest {msg.RequestId}");

            if (msg.TopologyHash != _topologyHash)
            {
                var ownerAddress = _memberHashRing.GetOwnerMemberByIdentity(msg.Identity);

                if (ownerAddress != _myAddress)
                {
                    if (_config.DeveloperLogging)
                        Console.WriteLine($"Forwarding ActivationRequest {msg.RequestId} to {ownerAddress}");

                    var ownerPid = PartitionManager.RemotePartitionIdentityActor(ownerAddress);
                    Logger.LogWarning("Tried to spawn on wrong node, forwarding");
                    context.Forward(ownerPid);

                    return Task.CompletedTask;
                }
            }

            //Check if exist in current partition dictionary
            if (_partitionLookup.TryGetValue(msg.ClusterIdentity, out var pid))
            {
                if (_config.DeveloperLogging)
                    Console.WriteLine($"Found existing activation for {msg.RequestId}");

                context.Respond(new ActivationResponse {Pid = pid});
                return Task.CompletedTask;
            }

            //only activate members when we are all in sync
            // var c = await _cluster.MemberList.TopologyConsensus(CancellationTokens.FromSeconds(5));
            //
            // if (!c)
            // {
            //     Console.WriteLine("No consensus " + _cluster.System.Id);
            // }

            //Get activator
            var activatorAddress = _cluster.MemberList.GetActivator(msg.Kind, context.Sender!.Address)?.Address;

            if (string.IsNullOrEmpty(activatorAddress))
            {
                if (_config.DeveloperLogging)
                    Console.Write("?");
                //No activator currently available, return unavailable
                Logger.LogWarning("No members currently available for kind {Kind}", msg.Kind);
                context.Respond(new ActivationResponse
                    {
                        Failed = true
                    }
                );
                return Task.CompletedTask;
            }

            //What is this?
            //in case the actor of msg.Name is not yet spawned. there could be multiple re-entrant
            //messages requesting it, we just reuse the same task for all those
            //once spawned, the key is removed from this dict
            if (!_spawns.TryGetValue(msg.ClusterIdentity, out var res))
            {
                res = SpawnRemoteActor(msg, activatorAddress);
                _spawns.Add(msg.ClusterIdentity, res);
            }

            //execution ends here. context.ReenterAfter is invoked once the task completes
            //but still within the actors sequential execution
            //but other messages could have been processed in between

            if (_config.DeveloperLogging)
                Console.Write("S"); //spawned
            //Await SpawningProcess
            context.ReenterAfter(
                res,
                rst => {
                    try
                    {
                        if (rst.IsCompletedSuccessfully)
                        {
                            var response = rst.Result;

                            if (_config.DeveloperLogging)
                                Console.Write("R"); //reentered

                            if (_partitionLookup.TryGetValue(msg.ClusterIdentity, out pid))
                            {
                                if (_config.DeveloperLogging)
                                    Console.Write("C"); //cached

                                if (response.Pid is not null && !response.Pid.Equals(pid))
                                {
                                    context.Stop(response.Pid); // Stop duplicate activation
                                }

                                _spawns.Remove(msg.ClusterIdentity);
                                context.Respond(new ActivationResponse {Pid = pid});
                                return Task.CompletedTask;
                            }

                            if (response?.Pid != null)
                            {
                                if (_config.DeveloperLogging)
                                    Console.Write("A"); //activated

                                if (response.TopologyHash != _topologyHash) // Topology changed between request and response
                                {
                                    activatorAddress = _cluster.MemberList.GetActivator(msg.Kind, context.Sender!.Address)?.Address;

                                    if (_myAddress != activatorAddress)
                                    {
                                        //TODO: Stop it or handover?
                                        Logger.LogWarning("Misplaced spawn: {ClusterIdentity}, {Pid}", msg.ClusterIdentity, response.Pid);
                                    }
                                }

                                _partitionLookup[msg.ClusterIdentity] = response.Pid;
                                _spawns.Remove(msg.ClusterIdentity);
                                context.Respond(response);

                                if (_waitForInflightActivations is not null)
                                {
                                    SetReadyToRebalanceIfNoMoreWaitingSpawns();
                                }

                                return Task.CompletedTask;
                            }
                        }

                        else
                        {
                            Logger.LogError(rst.Exception, "Spawn task failed");
                        }
                    }
                    catch (Exception x)
                    {
                        Logger.LogError(x, "Spawning failed");
                    }

                    if (_config.DeveloperLogging)
                        Console.Write("F"); //failed
                    _spawns.Remove(msg.ClusterIdentity);
                    context.Respond(new ActivationResponse {Failed = true});
                    return Task.CompletedTask;
                }
            );
            return Task.CompletedTask;
        }

        private void ActivateAfterConsensus(ActivationRequest msg, IContext context)
            => context.ReenterAfter(_cluster.MemberList.TopologyConsensus(CancellationToken.None), _ => OnActivationRequest(msg, context));

        // private void HandleMisplacedIdentity(ActivationRequest msg, ActivationResponse response, string? activatorAddress, IContext context)
        // {
        //     _spawns.Remove(msg.ClusterIdentity);
        //     if (activatorAddress is null)
        //     {
        //         context.Stop(response.Pid); // We could possibly move the activation to the new owner?
        //         RespondWithFailure(context);
        //     }
        //     else
        //     {
        //         var pid = PartitionManager.RemotePartitionIdentityActor(activatorAddress);
        //         context.RequestReenter<ActivationResponse>(pid, new ActivationHandover
        //         {
        //             ClusterIdentity = msg.ClusterIdentity,
        //             RequestId = msg.RequestId,
        //             TopologyHash = msg.TopologyHash,
        //             Pid = response.Pid
        //         }, responseTask => {
        //             if (responseTask.IsCompletedSuccessfully)
        //             {
        //                 context.Respond(responseTask.Result);
        //             }
        //             else
        //             {
        //                 context.Stop(response.Pid);
        //                 RespondWithFailure(context);
        //             }
        //             
        //             
        //             return Task.CompletedTask;
        //         }, CancellationTokens.WithTimeout(_identityHandoverTimeout));
        //     }
        // }

        private static void RespondWithFailure(IContext context) => context.Respond(new ActivationResponse {Failed = true});

        private async Task<ActivationResponse> SpawnRemoteActor(ActivationRequest req, string activatorAddress)
        {
            try
            {
                Logger.LogDebug("Spawning Remote Actor {Activator} {Identity} {Kind}", activatorAddress, req.Identity,
                    req.Kind
                );
                var timeout = _cluster.Config.TimeoutTimespan;
                var activatorPid = PartitionManager.RemotePartitionPlacementActor(activatorAddress);

                var res = await _cluster.System.Root.RequestAsync<ActivationResponse>(activatorPid, req, timeout);
                return res;
            }
            catch
            {
                return new ActivationResponse
                {
                    Failed = true
                };
            }
        }
    }

    record PartitionsRebalanced(Dictionary<ClusterIdentity, PID> OwnedActivations, ulong TopologyHash);
}