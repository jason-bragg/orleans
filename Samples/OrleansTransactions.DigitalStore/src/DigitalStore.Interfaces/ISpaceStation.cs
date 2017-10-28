using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DigitalStore.Interfaces
{
    /// <summary>
    /// A non-player space station
    /// </summary>
    public interface ISpaceStation : Orleans.IGrainWithIntegerKey
    {
        /// <summary>
        /// Peek at station current inventory, may change by the time you choose to trade!
        /// </summary>
        Task<Dictionary<Product, Stock>> PeekAtInventory();

        /// <summary>
        /// Buy product from merchant
        /// </summary>
        /// <returns>buy price</returns>
        Task<long> Buy(Product product, int quantity);

        /// <summary>
        /// Sell product to merchant
        /// </summary>
        /// <returns>sell price</returns>
        Task<long> Sell(Product product, int quantity);

    }

    [Serializable]
    public class Stock
    {
        Stock(int count, long price)
        {
            this.Count = count;
            this.BuyPrice = price;
            this.SellPrice = price;
        }

        public int Count { get; }
        public long BuyPrice { get; }
        public long SellPrice { get; }
    }
}
