using System;

namespace Orleans.Streams
{
    public interface IRecoverableStreamStoragePolicy
    {
        TimeSpan GetReadBackoff(AdvancedStorageReadResultCode resultCode, int attempts);

        bool ShouldBackoffOnWriteWithAmbiguousResult { get; }

        bool ShouldReloadOnWriteWithAmbiguousResult { get; } // Need metrics to decide on this. Probably tech specific. TODO: Maybe this should be a threshold

        TimeSpan GetWriteBackoff(AdvancedStorageWriteResultCode resultCode, int attempts);
    }
}