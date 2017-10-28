using System;
using System.Threading.Tasks;

namespace DigitalStore.Interfaces
{
    /// <summary>
    /// Space Trader!
    /// </summary>
    public interface ISpaceTrader : Orleans.IGrainWithIntegerKey
    {
        /// <summary>
        /// Ship product from one station to another.
        /// </summary>
        Task<TradeResult> ShipProduct(Product product, int quantity, Station from, Station to);
        Task<long> GetBallance();
    }

    [Serializable]
    public class TradeResult
    {
        public bool Pirated { get; }
        public string CaptainsMessage { get; }
        public long BuyPrice { get; }
        public long SellPrice { get; }
    }
}