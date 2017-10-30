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
        private ulong balance = 1000;

        Task<ulong> ISpaceTrader.GetBalance()
        {
            return Task.FromResult(this.balance);
        }

        async Task<TradeResult> ISpaceTrader.ShipProduct(Product product, uint quantity, string from, string to)
        {
            ISpaceStation fromStation = GrainFactory.GetGrain<ISpaceStation>(from);
            ISpaceStation toStation = GrainFactory.GetGrain<ISpaceStation>(to);
            ulong buyPrice = await fromStation.Buy(product, quantity);
            if (buyPrice > this.balance)
                throw new InvalidOperationException("Not enough money");
            this.balance -= buyPrice;
            ulong sellPrice = await toStation.Sell(product, quantity);
            this.balance += sellPrice;
            return new TradeResult()
            {
                Pirated = false,
                CaptainsMessage = buyPrice < sellPrice ? "profit" : "loss",
                BuyPrice = buyPrice,
                SellPrice = sellPrice
            };
        }
    }
}
