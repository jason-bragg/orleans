using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Threading;
using System.Threading.Tasks;
using DigitalStore.Interfaces;
using Microsoft.Extensions.Logging;

namespace OrleansClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        static int Main(string[] args)
        {
            var initializeClient = StartClient();
            initializeClient.Wait();
            var client = initializeClient.Result;
            DoClientWork(client).Wait();
            return 0;
        }

        private static Task<IClusterClient> StartClient()
        {
            try
            {
                return InitializeWithRetries();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Orleans client initialization failed failed due to {ex}");

                Console.ReadLine();
                return Task.FromException<IClusterClient>(ex);
            }
        }

        private static async Task<IClusterClient> InitializeWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    ClientConfiguration config = ClientConfiguration.LocalhostSilo();
                    client = new ClientBuilder()
                        .UseConfiguration(config)
                        .AddApplicationPartsFromReferences(typeof(ITraderBot).Assembly)
                        .ConfigureLogging(logging => logging.AddConsole())
                        .Build();

                    await client.Connect();
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine($"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        throw;
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }
            }

            return client;
        }

        private static async Task DoClientWork(IClusterClient client)
        {
            // example of calling grains from the initialized client
            var bobBot = client.GetGrain<ITraderBot>("bob");
            await bobBot.StartTrading("Sol");
            Console.WriteLine("Press Enter to terminate...");
            Console.ReadLine();
            await bobBot.StopTrading();
        }
    }
}
