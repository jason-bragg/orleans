using System;
using System.Threading.Tasks;
using Orleans.Core;

namespace Orleans.Streams
{
    public class AdvancedStorageSimpleStorageAdapter<TState> : IAdvancedStorage<TState>
        where TState : new()
    {
        private readonly IStorage<TState> simpleStorage;

        public AdvancedStorageSimpleStorageAdapter(IStorage<TState> simpleStorage)
        {
            if (simpleStorage == null) { throw new ArgumentNullException(nameof(simpleStorage)); }

            this.simpleStorage = simpleStorage;
        }

        public TState State
        {
            get => this.simpleStorage.State;
            set => this.simpleStorage.State = value;
        }

        public string ETag => this.simpleStorage.Etag;

        public async Task<AdvancedStorageReadResultCode> ReadStateAsync()
        {
            await this.simpleStorage.ReadStateAsync();

            return AdvancedStorageReadResultCode.Success;
        }

        public async Task<AdvancedStorageWriteResultCode> WriteStateAsync()
        {
            await this.simpleStorage.WriteStateAsync();

            return AdvancedStorageWriteResultCode.Success;
        } 
    }
}