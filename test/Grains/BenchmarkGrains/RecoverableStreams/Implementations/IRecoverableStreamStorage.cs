using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal interface IRecoverableStreamStorage<TState>
    {
        RecoverableStreamState<TState> State { get; set; }

        Task Load();

        Task<bool> Save();

        Task<(bool persisted, bool fastForwardRequested)> CheckpointIfOverdue();
    }
}