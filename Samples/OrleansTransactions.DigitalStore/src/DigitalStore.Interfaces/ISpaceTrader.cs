using System;
using System.Threading.Tasks;
using Orleans;

namespace DigitalStore.Interfaces
{
    /// <summary>
    /// Space Trader!
    /// </summary>
    public interface ISpaceTrader : IGrainWithStringKey
    {
        Task<TradeResult> ShipProduct(Product product, uint quantity, string from, string to);
        Task<ulong> GetBalance();
    }

    [Serializable]
    public class TradeResult
    {
        public bool Pirated { get; set; }
        public string CaptainsMessage { get; set; }
        public ulong BuyPrice { get; set; }
        public ulong SellPrice { get; set; }
    }
}