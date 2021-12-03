// -----------------------------------------------------------------------
// <copyright file="PartitionPlacementActor.cs" company="Asynkron AB">
//      Copyright (C) 2015-2020 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Partition
{
    class PartitionPlacementActor : IActor
    {
        private readonly Cluster _cluster;
        private static readonly ILogger Logger = Log.CreateLogger<PartitionPlacementActor>();

        //pid -> the actor that we have created here
        //kind -> the actor kind
        private readonly Dictionary<ClusterIdentity, PID> _myActors = new();
        private readonly PartitionConfig _config;

        public PartitionPlacementActor(Cluster cluster, PartitionConfig config)
        {
            _cluster = cluster;
            _config = config;
        }

        public Task ReceiveAsync(IContext context) =>
            context.Message switch
            {
                Terminated msg              => Terminated(msg),
                IdentityHandoverRequest msg => IdentityHandoverRequest(context, msg),
                ActivationRequest msg       => ActivationRequest(context, msg),
                _                           => Task.CompletedTask
            };

        private Task Terminated(Terminated msg)
        {
            //TODO: if this turns out to be perf intensive, lets look at optimizations for reverse lookups
            var (clusterIdentity, pid) = _myActors.FirstOrDefault(kvp => kvp.Value.Equals(msg.Who));

            var activationTerminated = new ActivationTerminated
            {
                Pid = pid,
                ClusterIdentity = clusterIdentity,
            };

            _cluster.MemberList.BroadcastEvent(activationTerminated);

            // var ownerAddress = _rdv.GetOwnerMemberByIdentity(clusterIdentity.Identity);
            // var ownerPid = PartitionManager.RemotePartitionIdentityActor(ownerAddress);
            //
            // context.Send(ownerPid, activationTerminated);
            _myActors.Remove(clusterIdentity);
            return Task.CompletedTask;
        }

        //this is pure, we do not change any state or actually move anything
        //the requester also provide its own view of the world in terms of members
        //TLDR; we are not using any topology state from this actor itself
        private Task IdentityHandoverRequest(IContext context, IdentityHandoverRequest msg)
        {
            var count = 0;
            var response = new IdentityHandoverResponse();
            var requestAddress = context.Sender!.Address;

            //use a local selector, which is based on the requesters view of the world
            var memberHashRing = new MemberHashRing(msg.Members);

            var chunkId = 1;
            var chunkSize = _config.HandoverChunkSize;
            var cancelRebalance = new CancellationTokenSource();
            var outOfBandResponseHandler = context.System.Root.Spawn(AbortOnDeadLetter(cancelRebalance));

            try
            {
                foreach (var (clusterIdentity, pid) in _myActors)
                {
                    //who owns this identity according to the requesters memberlist?
                    var ownerAddress = memberHashRing.GetOwnerMemberByIdentity(clusterIdentity);

                    //this identity is not owned by the requester
                    if (ownerAddress != requestAddress) continue;

                    Logger.LogDebug("Transfer {Identity} to {NewOwnerAddress} -- {TopologyHash}", clusterIdentity, ownerAddress,
                        msg.TopologyHash
                    );

                    var actor = new Activation {ClusterIdentity = clusterIdentity, Pid = pid};
                    response.Actors.Add(actor);
                    count++;
                    
                    if (count % chunkSize == 0)
                    {
                        if (cancelRebalance.IsCancellationRequested)
                        {
                            return Task.CompletedTask;
                        }
                        response.ChunkId = chunkId++;
                        context.Request(context.Sender, response, outOfBandResponseHandler);
                        response = new IdentityHandoverResponse();
                    }
                }
                if (cancelRebalance.IsCancellationRequested)
                {
                    return Task.CompletedTask;
                }
                response.ChunkId = chunkId;
                response.Final = true;

                context.Request(context.Sender, response);

                Logger.LogDebug("Transferred {Count} actor ownership to other members", count);
            }
            finally
            {
                if (cancelRebalance.IsCancellationRequested)
                {
                    Logger.LogInformation("Cancelled rebalance: {@IdentityHandoverRequest}", msg);
                }
                context.Stop(outOfBandResponseHandler);
            }
            return Task.CompletedTask;
        }

        private Props AbortOnDeadLetter(CancellationTokenSource cts) => Props.FromFunc(responseContext => {
                // Node lost or rebalance cancelled because of topology changes
                if (responseContext.Message is DeadLetterResponse)
                {
                    cts.Cancel();
                }

                return Task.CompletedTask;
            }
        );

        private Task ActivationRequest(IContext context, ActivationRequest msg)
        {
            try
            {
                if (_myActors.TryGetValue(msg.ClusterIdentity, out var existing))
                {
                    if (_config.DeveloperLogging)
                        Console.WriteLine($"Activator got request for existing activation {msg.RequestId}");
                    //this identity already exists
                    var response = new ActivationResponse
                    {
                        Pid = existing
                    };
                    context.Respond(response);
                }
                else
                {
                    if (_config.DeveloperLogging)
                        Console.WriteLine($"Activator got request for new activation {msg.RequestId}");
                    var clusterKind = _cluster.GetClusterKind(msg.ClusterIdentity.Kind);
                    //this actor did not exist, lets spawn a new activation

                    //spawn and remember this actor
                    //as this id is unique for this activation (id+counter)
                    //we cannot get ProcessNameAlreadyExists exception here

                    var clusterProps = clusterKind.Props.WithClusterIdentity(msg.ClusterIdentity);

                    var pid = context.SpawnPrefix(clusterProps, msg.ClusterIdentity.Identity);

                    _myActors[msg.ClusterIdentity] = pid;

                    var response = new ActivationResponse
                    {
                        Pid = pid
                    };
                    context.Respond(response);
                    if (_config.DeveloperLogging)
                        Console.WriteLine($"Activated {msg.RequestId}");
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to spawn {Kind}/{Identity}", msg.Kind, msg.Identity);
                var response = new ActivationResponse
                {
                    Pid = null
                };
                context.Respond(response);
            }

            return Task.CompletedTask;
        }
    }
}