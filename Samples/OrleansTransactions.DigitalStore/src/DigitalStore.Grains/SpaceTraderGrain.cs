using System;
using System.Threading.Tasks;
using DigitalStore.Interfaces;
using Orleans;

namespace DigitalStore.Grains
{
    /// <summary>
    /// Player space trader grain
    /// </summary>
    internal class SpaceTraderGrain : Grain, ISpaceTrader
    {
        Task<long> ISpaceTrader.GetBallance()
        {
            throw new NotImplementedException();
        }

        Task<TradeResult> ISpaceTrader.ShipProduct(Product product, int quantity, Station from, Station to)
        {
            throw new NotImplementedException();
        }
    }
}
