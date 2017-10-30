using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    public class RandoTraderBot : Grain, ITraderBot
    {
        private static readonly TimeSpan DefaultTradeInterval = TimeSpan.FromSeconds(10);
        private readonly Random random = new Random((int)(DateTime.UtcNow.Ticks & 0xFFFFFFFF));
        private readonly ILoggerFactory loggerFactory;
        private ISpaceTrader trader;
        private IStarSystem system;
        private IDisposable tradeTimer;
        private ILogger logger;

        public RandoTraderBot(ILoggerFactory loggerFactory)
        {
            this.loggerFactory = loggerFactory;
        }

        public override Task OnActivateAsync()
        {
            this.logger = this.loggerFactory.CreateLogger($"{GetType().FullName}.{this.GetPrimaryKeyString()}");
            return Task.CompletedTask;
        }


        public Task StartTrading(string system, TimeSpan? interval = null)
        {
            this.trader = GrainFactory.GetGrain<ISpaceTrader>(this.GetPrimaryKeyString());
            this.system = GrainFactory.GetGrain<IStarSystem>(system);
            if(this.tradeTimer != null)
            {
                throw new InvalidOperationException("Already trading");
            }
            TimeSpan tradeInterval = interval ?? DefaultTradeInterval;
            this.tradeTimer = RegisterTimer(Trade, null, tradeInterval, tradeInterval);
            return Task.CompletedTask;
        }

        public Task StopTrading()
        {
            this.tradeTimer?.Dispose();
            this.tradeTimer = null;
            return Task.CompletedTask;
        }

        private async Task Trade(object unused)
        {
            // get stations in system
            IList<StationDescription> stations = await this.system.GetStations();

            // in not enough to trade with, do nothing
            if (stations.Count < 2) return;

            // get from station name
            int index = this.random.Next(0, stations.Count);
            string from = stations[index].Name;
            stations.RemoveAt(index);
            // get to station name
            index = this.random.Next(0, stations.Count);
            string to = stations[index].Name;
            stations.RemoveAt(index);

            ISpaceStation fromStation = GrainFactory.GetGrain<ISpaceStation>(from);
            Dictionary<Product,Stock> fromInventory = await fromStation.PeekAtInventory();

            ISpaceStation toStation = GrainFactory.GetGrain<ISpaceStation>(to);
            Dictionary<Product, Stock> toInventory = await toStation.PeekAtInventory();

            KeyValuePair<Product, Stock>? buy = null;
            ulong profit = 0;
            foreach (KeyValuePair<Product, Stock> fromStock in fromInventory)
            {
                Stock toStock = toInventory[fromStock.Key];
                if (toStock.SellPrice > fromStock.Value.BuyPrice &&
                    fromStock.Value.BuyPrice - toStock.SellPrice > profit)
                {
                    buy = fromStock;
                    profit = fromStock.Value.BuyPrice - toStock.SellPrice;
                }
            }

            // noting profitable, try again later
            if (!buy.HasValue) return;

            ulong balance = await this.trader.GetBalance();
            uint quantity = Math.Min((uint)(balance / buy.Value.Value.BuyPrice), buy.Value.Value.Quantity);

            try
            {
                TradeResult result = await this.trader.ShipProduct(buy.Value.Key, quantity, from, to);
                this.logger.LogInformation("Traded {0} of {1} from {2} to {3}.  Note: {4}", quantity, buy.Value.Key, from, to, result.CaptainsMessage);
            } catch(Exception ex)
            {
                this.logger.LogWarning("Trade {0} of {1} from {2} to {3} failed.  Ex: {4}", quantity, buy.Value.Key, from, to, ex);
            }
        }
    }
}
