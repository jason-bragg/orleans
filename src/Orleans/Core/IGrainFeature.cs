using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    public interface IGrainFeature
    {
    }

    public interface IStorageState<TState> : IGrainFeature
    {
        TState State { get; }
        Task ClearStateAsync();
        Task WriteStateAsync();
        Task ReadStateAsync();
    }

    public interface ITransactionMonitor<TState>
    {
        Task OnCommited(TState state);
        Task OnAborted(TState state);
    }

    public interface ITransactionalState<TState> : IGrainFeature
    {
        TState State { get; }
        ITransactionMonitor<TState> Monitor { set; }
        void Save();
    }

    [AttributeUsage(System.AttributeTargets.Property)]
    public abstract class FeatureAttribute : Attribute
    {
        public string Name { get; }
        public FeatureAttribute(string featureName)
        {
            this.Name = featureName;
        }
    }
}
