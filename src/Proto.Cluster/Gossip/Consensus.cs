// -----------------------------------------------------------------------
// <copyright file="ClusterConsensus.cs" company="Asynkron AB">
//      Copyright (C) 2015-2021 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Proto.Cluster.Gossip
{
    public interface IConsensusHandle<T> : IDisposable
    {
        Task<(bool consensus, T value)> GossipConsensus(CancellationToken ct);
    }

    internal class GossipConsensusHandleHandle<T> : IConsensusHandle<T>
    {
        private volatile TaskCompletionSource<T> _consensus = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly Action _deregister;

        public GossipConsensusHandleHandle(Action deregister) => _deregister = deregister;

        internal void TrySetConsensus(object consensus)
        {
            if (_consensus.Task.IsCompleted && _consensus.Task.Result?.Equals(consensus) != true)
            {
                TryResetConsensus();
            }

            //if not set, set it, if already set, keep it set
            _consensus.TrySetResult((T) consensus);
        }

        internal void TryResetConsensus()
        {
            //only replace if the task is completed
            if (_consensus.Task.IsCompleted)
            {
                _consensus = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public async Task<(bool consensus, T value)> GossipConsensus(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                var t = _consensus.Task;
                // ReSharper disable once MethodSupportsCancellation
                await Task.WhenAny(t, Task.Delay(500));
                if (t.IsCompleted)
                    return (true, t.Result);
            }

            return (false, default);
        }

        public void Dispose() => _deregister();
    }
}