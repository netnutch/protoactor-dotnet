// -----------------------------------------------------------------------
// <copyright file="ClusterHeartBeat.cs" company="Asynkron AB">
//      Copyright (C) 2015-2020 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Proto.Logging;

namespace Proto.Cluster.Gossip
{
    public record GossipUpdate(string MemberId, string Key, Any Value, long SequenceNumber);

    public record GetGossipStateRequest(string Key);

    public record GetGossipStateResponse(ImmutableDictionary<string, Any> State);

    public record SetGossipStateKey(string Key, IMessage Value);

    public record SendGossipStateRequest;

    public record SendGossipStateResponse;

    internal record AddConsensusCheck(GossipActor.ConsensusCheck Check);

    internal record RemoveConsensusCheck(string Id, string key);

    internal record ConsensusCommandAck;

    public class Gossiper
    {
        public const string GossipActorName = "gossip";
        private readonly Cluster _cluster;
        private readonly RootContext _context;

        private static readonly ILogger Logger = Log.CreateLogger<Gossiper>();
        private PID _pid = null!;

        public Gossiper(Cluster cluster)
        {
            _cluster = cluster;
            _context = _cluster.System.Root;
        }

        public async Task<ImmutableDictionary<string, T>> GetState<T>(string key) where T : IMessage, new()
        {
            _context.System.Logger()?.LogDebug("Gossiper getting state from {Pid}", _pid);

            var res = await _context.RequestAsync<GetGossipStateResponse>(_pid, new GetGossipStateRequest(key));

            var dict = res.State;
            var typed = ImmutableDictionary<string, T>.Empty;

            foreach (var (k, value) in dict)
            {
                typed = typed.SetItem(k, value.Unpack<T>());
            }

            return typed;
        }

        public void SetState(string key, IMessage value)
        {
            Logger.LogDebug("Gossiper setting state to {Pid}", _pid);
            _context.System.Logger()?.LogDebug("Gossiper setting state to {Pid}", _pid);

            if (_pid == null)
            {
                return;
            }

            _context.Send(_pid, new SetGossipStateKey(key, value));
        }

        internal Task StartAsync()
        {
            var props = Props.FromProducer(() => new GossipActor(_cluster.Config.GossipRequestTimeout));
            _pid = _context.SpawnNamed(props, GossipActorName);
            Logger.LogInformation("Started Cluster Gossip");
            _ = SafeTask.Run(GossipLoop);
            return Task.CompletedTask;
        }

        private async Task GossipLoop()
        {
            Logger.LogInformation("Starting gossip loop");
            await Task.Yield();

            while (!_cluster.System.Shutdown.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay((int) _cluster.Config.GossipInterval.TotalMilliseconds);
                    SetState("heartbeat", new MemberHeartbeat());
                    await SendStateAsync();
                }
                catch (Exception x)
                {
                    Logger.LogError(x, "Gossip loop failed");
                }
            }
        }

        public Task<IConsensusHandle<T>> RegisterConsensusCheck<T>(string key) where T : IMessage, new()
            => RegisterConsensusCheck<T, T>(key, state => state);

        public async Task<IConsensusHandle<TV>> RegisterConsensusCheck<T, TV>(string key, Func<T, TV> getValue) where T : IMessage, new()
        {
            var id = Guid.NewGuid().ToString("N");
            var handle = new GossipConsensusHandleHandle<TV>(() => _context.RequestAsync<ConsensusCommandAck>(_pid, new RemoveConsensusCheck(id, key))
            );

            await _context.RequestAsync<ConsensusCommandAck>(_pid,
                new AddConsensusCheck(new GossipActor.ConsensusCheck(id, CheckConsensus)), CancellationToken.None
            );

            return handle;

            void CheckConsensus(GossipState state, ImmutableHashSet<string> members)
            {
                var (consensus, value) = GossipStateManagement.CheckConsensus(null, state, _cluster.System.Id, members, key, getValue);

                if (consensus)
                {
                    handle.TrySetConsensus(value!);
                }
                else
                {
                    handle.TryResetConsensus();
                }
            }
        }
        
        private async Task SendStateAsync()
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (_pid == null)
            {
                //just make sure a cluster client cant send
                return;
            }

            try
            {
                await _context.RequestAsync<SendGossipStateResponse>(_pid, new SendGossipStateRequest(), CancellationTokens.FromSeconds(5));
            }
            catch (DeadLetterException)
            {
            }
            catch (OperationCanceledException)
            {
            }
#pragma warning disable RCS1075
            catch (Exception)
#pragma warning restore RCS1075
            {
                //TODO: log
            }
        }

        internal Task ShutdownAsync()
        {
            Logger.LogInformation("Shutting down heartbeat");
            _context.Stop(_pid);
            Logger.LogInformation("Shut down heartbeat");
            return Task.CompletedTask;
        }
    }
}