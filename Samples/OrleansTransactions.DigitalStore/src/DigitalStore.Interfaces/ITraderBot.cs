using Orleans;
using System;
using System.Threading.Tasks;

namespace DigitalStore.Interfaces
{
    public interface ITraderBot : IGrainWithStringKey
    {
        Task StartTrading(string system, TimeSpan? interval = null);
        Task StopTrading();
    }
}
