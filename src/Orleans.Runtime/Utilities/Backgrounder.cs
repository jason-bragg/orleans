using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Timers.Internal;
using Orleans.Utilities;

namespace Orleans.Runtime.Utilities
{
    internal class Backgrounder : IBackgrounder, ILifecycleParticipant<IGrainLifecycle>
    {
        private readonly BackgrounderOptions _options;
        private readonly IGrainActivationContext _context;
        private readonly ITimerManager _timerManager;
        private readonly ILogger<Backgrounder> _logger;
        private readonly CancellationTokenSource _deactivating;
        private readonly List<BackgroundWork> _pending;

        public Backgrounder(IOptions<BackgrounderOptions> options, IGrainActivationContext context, ITimerManager timerManager, ILogger<Backgrounder> logger)
        {
            _options = options.Value;
            _context = context;
            _timerManager = timerManager;
            _logger = logger;
            _deactivating = new CancellationTokenSource();
            _pending = new List<BackgroundWork>();
        }

        public Task<T> Run<T>(Func<CancellationToken, Task<T>> fetch)
        {
            var work = new BackgroundWork();
            work.OnComplete = () => _pending.Remove(work);
            return work.Fetch(() => fetch(_deactivating.Token));
        }

        public void Participate(IGrainLifecycle lifecycle)
        {
            // Signal we're deactivating at first chance ('last' stage during shutdown).
            lifecycle.Subscribe(nameof(Backgrounder), GrainLifecycleStage.Last, NoOp, SignalDeactivating);
            // Wait for pending work prior to on deactivate logic, for cleaner grain developer experience.
            lifecycle.Subscribe(nameof(Backgrounder), GrainLifecycleStage.Activate + 1, NoOp, AwaitPending);
        }

        private async Task AwaitPending(CancellationToken ct)
        {
            if (!_deactivating.IsCancellationRequested) _deactivating.Cancel(false);
            if (ct.IsCancellationRequested) return;
            if (_pending.Count == 0) return;
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.Debug("Background work found while deactivating.  GrainType: {GrainType}, GrainId: {GrainId}, Count: {Count}",
                    _context.GrainType, _context.GrainIdentity, _pending.Count);
            }
            Task pendingWork = Task.WhenAll(_pending.Select(p => p.Pending));
            pendingWork.Ignore();
            var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(pendingWork, _timerManager.Delay(_options.BackgroundDeactivationTimeout));

            // If pending completed before the timeout, await the completed result to throw any errors
            if (pendingWork == completedTask)
            {
                timeoutCancellationTokenSource.Cancel();
                await pendingWork;
                return;
            }
            _logger.LogWarning("Background work did not complete within deactivation timeout. GrainType: {GrainType}, GrainId: {GrainId}, BackgroundDeactivationTimeout: {BackgroundDeactivationTimeout}",
                _context.GrainType, _context.GrainIdentity, _options.BackgroundDeactivationTimeout);
        }

        private Task NoOp(CancellationToken ct) => Task.CompletedTask;

        private Task SignalDeactivating(CancellationToken ct)
        {
            if (!_deactivating.IsCancellationRequested) _deactivating.Cancel(false);
            return Task.CompletedTask;
        }

        private class BackgroundWork
        {
            public Action OnComplete { set; private get; }
            public Task Pending { get; private set; }

            public Task<T> Fetch<T>(Func<Task<T>> fetch)
            {
                Task<T> work = WatchFetch(fetch);
                this.Pending = work;
                return work;
            }

            private async Task<T> WatchFetch<T>(Func<Task<T>> fetch)
            {
                try
                {
                    return await fetch();
                }
                finally
                {
                    // clear ourselves from tracked work when complete, error or no.
                    OnComplete();
                }
            }
        }

        public static IBackgrounder Create(IServiceProvider services)
        {
            var grainContext = services.GetRequiredService<IGrainActivationContext>();
            var backgrounder = ActivatorUtilities.CreateInstance<Backgrounder>(services);
            backgrounder.Participate(grainContext.ObservableLifecycle);
            return backgrounder;
        }
    }
}
