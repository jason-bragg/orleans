using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace DigitalStore.Interfaces
{
    /// <summary>
    /// A non-player space station
    /// </summary>
    public interface ISpaceStation : IGrainWithStringKey
    {
        /// <summary>
        /// Peek at station current inventory, may change by the time you choose to trade!
        /// </summary>
        Task<Dictionary<Product, Stock>> PeekAtInventory();

        /// <summary>
        /// Buy product from merchant
        /// </summary>
        /// <returns>buy price</returns>
        Task<ulong> Buy(Product product, uint quantity);

        /// <summary>
        /// Sell product to merchant
        /// </summary>
        /// <returns>sell price</returns>
        Task<ulong> Sell(Product product, uint quantity);

    }

    [Serializable]
    public class Stock
    {
        private const double Markup = 0.05;
        public Stock(uint quantity, ulong price)
        {
            this.Quantity = quantity;
            this.BuyPrice = (ulong)(price * (1.0 + Markup));
            this.SellPrice = (ulong)(price * (1.0 - Markup));
        }

        public uint Quantity { get; }
        public ulong BuyPrice { get; }
        public ulong SellPrice { get; }
    }
}
